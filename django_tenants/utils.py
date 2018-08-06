from contextlib import contextmanager
from django.conf import settings
from django.db import connections, DEFAULT_DB_ALIAS, transaction
from django.db.utils import ProgrammingError
from django.core.exceptions import ImproperlyConfigured
from psycopg2.extensions import AsIs


try:
    from django.apps import apps
    get_model = apps.get_model
except ImportError:
    from django.db.models.loading import get_model

from django.core import mail


def get_tenant_model():
    return get_model(settings.TENANT_MODEL)


def get_tenant_database_alias():
    return getattr(settings, 'TENANT_DB_ALIAS', DEFAULT_DB_ALIAS)


def get_public_schema_name():
    return getattr(settings, 'PUBLIC_SCHEMA_NAME', 'public')


def get_limit_set_calls():
    return getattr(settings, 'TENANT_LIMIT_SET_CALLS', False)


def get_clone_schema_owner():
    return getattr(settings, 'CLONE_SCHEMA_OWNER', 'postgres')


def get_creation_fakes_migrations():
    """
    If TENANT_CREATION_FAKES_MIGRATIONS, tenants will be created by cloning an existing schema
    specified by TENANT_CLONE_BASE
    """
    faked = getattr(settings, 'TENANT_CREATION_FAKES_MIGRATIONS', False)
    if faked:
        if not getattr(settings, 'TENANT_BASE_SCHEMA', False):
            raise ImproperlyConfigured(
                'You must specify a schema name in TENANT_BASE_SCHEMA if TENANT_CREATION_FAKES_MIGRATIONS is enabled.'
            )
    return faked


def get_tenant_base_schema():
    schema = getattr(settings, 'TENANT_BASE_SCHEMA', False)
    if schema:
        if not getattr(settings, 'TENANT_CREATION_FAKES_MIGRATIONS', False):
            raise ImproperlyConfigured(
                'TENANT_CREATION_FAKES_MIGRATIONS must be True to use TENANT_BASE_SCHEMA for cloning.'
            )
    return schema


@contextmanager
def schema_context(schema_name, include_public=True):
    connection = connections[get_tenant_database_alias()]
    previous_tenant = connection.tenant
    previous_include_public = connection.include_public_schema
    try:
        connection.set_schema(schema_name, include_public)
        yield
    finally:
        if previous_tenant is None:
            connection.set_schema_to_public()
        else:
            connection.set_tenant(previous_tenant, previous_include_public)


@contextmanager
def tenant_context(tenant, include_public=True):
    connection = connections[get_tenant_database_alias()]
    previous_tenant = connection.tenant
    previous_include_public = connection.include_public_schema
    try:
        connection.set_tenant(tenant, include_public)
        yield
    finally:
        if previous_tenant is None:
            connection.set_schema_to_public()
        else:
            connection.set_tenant(previous_tenant, previous_include_public)


def clean_tenant_url(url_string):
    """
    Removes the TENANT_TOKEN from a particular string
    """
    if hasattr(settings, 'PUBLIC_SCHEMA_URLCONF'):
        if (settings.PUBLIC_SCHEMA_URLCONF and
                url_string.startswith(settings.PUBLIC_SCHEMA_URLCONF)):
            url_string = url_string[len(settings.PUBLIC_SCHEMA_URLCONF):]
    return url_string


def remove_www_and_dev(hostname):
    """
    Legacy function - just in case someone is still using the old name
    """
    return remove_www(hostname)


def remove_www(hostname):
    """
    Removes www. from the beginning of the address. Only for
    routing purposes. www.test.com/login/ and test.com/login/ should
    find the same tenant.
    """
    if hostname.startswith("www."):
        return hostname[4:]

    return hostname


def django_is_in_test_mode():
    """
    I know this is very ugly! I'm looking for more elegant solutions.
    See: http://stackoverflow.com/questions/6957016/detect-django-testing-mode
    """
    return hasattr(mail, 'outbox')


def schema_exists(schema_name):
    connection = connections[get_tenant_database_alias()]
    cursor = connection.cursor()

    # check if this schema already exists in the db
    sql = 'SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s)'
    cursor.execute(sql, (schema_name, ))

    row = cursor.fetchone()
    if row:
        exists = row[0]
    else:
        exists = False

    cursor.close()

    return exists


def app_labels(apps_list):
    """
    Returns a list of app labels of the given apps_list
    """
    return [app.split('.')[-1] for app in apps_list]


def protect_case(schema_name):
    return '"' + schema_name + '"'


# Postgres' `clone_schema` adapted to work with schema names containing capital letters or `-`
# Source: IdanDavidi, https://stackoverflow.com/a/48732283/6412017
CLONE_SCHEMA_FUNCTION = """
-- Function: clone_schema(text, text)

-- DROP FUNCTION clone_schema(text, text);

CREATE OR REPLACE FUNCTION clone_schema(
    source_schema text,
    dest_schema text,
    include_recs boolean)
  RETURNS void AS
$BODY$

--  This function will clone all sequences, tables, data, views & functions from any existing schema to a new one
-- SAMPLE CALL:
-- SELECT clone_schema('public', 'new_schema', TRUE);

DECLARE
  src_oid          oid;
  tbl_oid          oid;
  func_oid         oid;
  table_rec        record;
  seq_rec          record;
  object           text;
  sequence_        text;
  table_           text;
  buffer           text;
  seq_buffer       text;
  table_buffer     text;
  srctbl           text;
  default_         text;
  column_          text;
  qry              text;
  dest_qry         text;
  v_def            text;
  seqval           bigint;
  sq_last_value    bigint;
  sq_max_value     bigint;
  sq_start_value   bigint;
  sq_increment_by  bigint;
  sq_min_value     bigint;
  sq_cache_value   bigint;
  sq_log_cnt       bigint;
  sq_is_called     boolean;
  sq_is_cycled     boolean;
  sq_cycled        char(10);

BEGIN

-- Check that source_schema exists
  SELECT oid INTO src_oid
    FROM pg_namespace
   WHERE nspname = source_schema;
  IF NOT FOUND
    THEN
    RAISE EXCEPTION 'source schema % does not exist!', source_schema;
    RETURN ;
  END IF;

-- Check that dest_schema does not yet exist
  PERFORM nspname
    FROM pg_namespace
   WHERE nspname = dest_schema;
  IF FOUND
    THEN
    RAISE EXCEPTION 'dest schema % already exists!', dest_schema;
    RETURN ;
  END IF;

  EXECUTE 'CREATE SCHEMA "' || dest_schema || '"';

-- Create tables
  FOR object IN
    SELECT TABLE_NAME::text
      FROM information_schema.tables
     WHERE table_schema = source_schema
       AND table_type = 'BASE TABLE'

  LOOP
    buffer := '"' || dest_schema || '".' || quote_ident(object);
    EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE "' || source_schema || '".' || quote_ident(object)
        || ' INCLUDING ALL);';

    IF include_recs
      THEN
      -- Insert records from source table
      EXECUTE 'INSERT INTO ' || buffer || ' SELECT * FROM "' || source_schema || '".' || quote_ident(object) || ';';
    END IF;

  END LOOP;

--  add FK constraint
  FOR qry IN
    SELECT 'ALTER TABLE "' || dest_schema || '".' || quote_ident(rn.relname)
            || ' ADD CONSTRAINT ' || quote_ident(ct.conname) || ' ' || pg_get_constraintdef(ct.oid) || ';'
      FROM pg_constraint ct
      JOIN pg_class rn ON rn.oid = ct.conrelid
     WHERE connamespace = src_oid
       AND rn.relkind = 'r'
       AND ct.contype = 'f'

    LOOP
      EXECUTE qry;

    END LOOP;

-- Create sequences
  FOR seq_rec IN
    SELECT
      s.sequence_name::text,
      table_name,
      column_name
    FROM information_schema.sequences s
    JOIN (
      SELECT
        substring(column_default from E'^nextval\\\\(''(?:[^"\'\']?.*["\'\']?\\\\.)?([^\'\']*)\'\'(?:::text|::regclass)?\\\\)')::text AS seq_name,
        table_name,
        column_name
      FROM information_schema.columns
      WHERE column_default LIKE 'nextval%'
        AND table_schema = source_schema
    ) c ON c.seq_name = s.sequence_name
    WHERE sequence_schema = source_schema
  LOOP
    seq_buffer := quote_ident(dest_schema) || '.' || quote_ident(seq_rec.sequence_name);

    RAISE NOTICE 'seq buffer %', seq_buffer;
    EXECUTE 'CREATE SEQUENCE ' || seq_buffer || ';';

    qry := 'SELECT last_value, max_value, start_value, increment_by, min_value, cache_value, log_cnt, is_cycled, is_called
              FROM "' || source_schema || '".' || quote_ident(seq_rec.sequence_name) || ';';
    EXECUTE qry INTO sq_last_value, sq_max_value, sq_start_value, sq_increment_by, sq_min_value, sq_cache_value, sq_log_cnt, sq_is_cycled, sq_is_called ;

    IF sq_is_cycled
      THEN
        sq_cycled := 'CYCLE';
    ELSE
        sq_cycled := 'NO CYCLE';
    END IF;

    EXECUTE 'ALTER SEQUENCE '   || seq_buffer
            || ' INCREMENT BY ' || sq_increment_by
            || ' MINVALUE '     || sq_min_value
            || ' MAXVALUE '     || sq_max_value
            || ' START WITH '   || sq_start_value
            || ' RESTART '      || sq_min_value
            || ' CACHE '        || sq_cache_value
            || ' OWNED BY '     || quote_ident(dest_schema ) || '.'
                                || quote_ident(seq_rec.table_name) || '.'
                                || quote_ident(seq_rec.column_name) || ' '
            || sq_cycled || ' ;' ;

    IF include_recs
        THEN
            EXECUTE 'SELECT setval(' || quote_literal(seq_buffer) || ', ' || sq_last_value || ', ' || sq_is_called || ');' ;
    ELSE
            EXECUTE 'SELECT setval(' || quote_literal(seq_buffer) || ', ' || sq_start_value || ', ' || sq_is_called || ');' ;
    END IF;

    table_buffer := quote_ident(dest_schema) || '.' || quote_ident(seq_rec.table_name);

    FOR table_rec IN
      SELECT column_name::text AS column_,
             REPLACE(column_default::text, source_schema, quote_ident(dest_schema)) AS default_
        FROM information_schema.COLUMNS
       WHERE table_schema = dest_schema
         AND TABLE_NAME = seq_rec.table_name
         AND column_default LIKE 'nextval(%' || seq_rec.sequence_name || '%::regclass)'
    LOOP
      EXECUTE 'ALTER TABLE ' || table_buffer || ' ALTER COLUMN ' || table_rec.column_ || ' SET DEFAULT nextval(' || quote_literal(seq_buffer) || '::regclass);';
    END LOOP;

  END LOOP;

-- Create views
  FOR object IN
    SELECT table_name::text,
           view_definition
      FROM information_schema.views
     WHERE table_schema = source_schema

  LOOP
    buffer := '"' || dest_schema || '".' || quote_ident(object);
    SELECT view_definition INTO v_def
      FROM information_schema.views
     WHERE table_schema = source_schema
       AND table_name = quote_ident(object);

    EXECUTE 'CREATE OR REPLACE VIEW ' || buffer || ' AS ' || v_def || ';' ;

  END LOOP;

-- Create functions
  FOR func_oid IN
    SELECT oid
      FROM pg_proc
     WHERE pronamespace = src_oid

  LOOP
    SELECT pg_get_functiondef(func_oid) INTO qry;
    SELECT replace(qry, source_schema, dest_schema) INTO dest_qry;
    EXECUTE dest_qry;

  END LOOP;

  RETURN;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
"""


def _create_clone_schema_function():
    """
    Will be created under the user 'postgres' by default.
    If you wish to create this under another user, specify the user name as CLONE_SCHEMA_OWNER in settings.
    :return:
    """
    owner = get_clone_schema_owner()
    connection = connections[get_tenant_database_alias()]
    cursor = connection.cursor()
    cursor.execute(CLONE_SCHEMA_FUNCTION)
    cursor.execute("ALTER FUNCTION clone_schema(text, text, boolean) OWNER TO %s;", (AsIs(owner),))
    cursor.close()


def clone_schema(base_schema_name, new_schema_name):
    """
    Creates a new schema `new_schema_name` as a clone of an existing schema `old_schema_name`.
    :param base_schema_name:
    :param new_schema_name:
    :return:
    """
    connection = connections[get_tenant_database_alias()]
    connection.set_schema_to_public()
    cursor = connection.cursor()

    # check if the clone_schema function already exists in the db
    try:
        cursor.execute("SELECT 'clone_schema'::regproc")
    except ProgrammingError:
        _create_clone_schema_function()
        transaction.commit()

    sql = 'SELECT clone_schema(%(base_schema)s, %(new_schema)s, TRUE)'
    cursor.execute(
        sql,
        {'base_schema': base_schema_name, 'new_schema': new_schema_name}
    )
    cursor.close()
