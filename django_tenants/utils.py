from contextlib import contextmanager
from django.conf import settings
from django.db import connections, DEFAULT_DB_ALIAS

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


def get_creation_fakes_migrations():
    return getattr(settings, 'TENANT_CREATION_FAKES_MIGRATIONS', False)


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
