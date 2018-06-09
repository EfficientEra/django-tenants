import re
import warnings
from django.conf import settings
from importlib import import_module

from django.core.exceptions import ImproperlyConfigured, ValidationError
from django_tenants.utils import get_public_schema_name, get_limit_set_calls, protect_case
from django_tenants.postgresql_backend.introspection import DatabaseSchemaIntrospection
import django.db.utils
import psycopg2
from psycopg2.extensions import AsIs


DatabaseError = django.db.utils.DatabaseError
IntegrityError = psycopg2.IntegrityError

ORIGINAL_BACKEND = getattr(settings, 'ORIGINAL_BACKEND', 'django.db.backends.postgresql_psycopg2')

original_backend = import_module(ORIGINAL_BACKEND + '.base')

EXTRA_SEARCH_PATHS = getattr(settings, 'PG_EXTRA_SEARCH_PATHS', [])

# from the postgresql doc
SQL_IDENTIFIER_RE = re.compile(r'^[_a-zA-Z][_a-zA-Z0-9]{,62}$')
SQL_SCHEMA_NAME_RESERVED_RE = re.compile(r'^pg_', re.IGNORECASE)


def _is_valid_identifier(identifier):
    return bool(SQL_IDENTIFIER_RE.match(identifier))


def _check_identifier(identifier):
    if not _is_valid_identifier(identifier):
        raise ValidationError("Invalid string used for the identifier.")


def _is_valid_schema_name(name):
    return _is_valid_identifier(name) and not SQL_SCHEMA_NAME_RESERVED_RE.match(name)


def _check_schema_name(name):
    if not _is_valid_schema_name(name):
        raise ValidationError("Invalid string used for the schema name.")


class DatabaseWrapper(original_backend.DatabaseWrapper):
    """
    Adds the capability to manipulate the search_path using set_tenant and set_schema_name
    """
    include_public_schema = True

    def __init__(self, *args, **kwargs):
        self.search_path_set = None
        self.tenant = None
        self.schema_name = None
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        # Use a patched version of the DatabaseIntrospection that only returns the table list for the
        # currently selected schema.
        self.introspection = DatabaseSchemaIntrospection(self)
        self.set_schema_to_public()

    def close(self):
        self.search_path_set = False
        super(DatabaseWrapper, self).close()

    def set_tenant(self, tenant, include_public=True):
        """
        Main API method to current database schema,
        but it does not actually modify the db connection.
        """
        self.tenant = tenant
        self._set_schema(tenant.schema_name, include_public)

    def set_schema(self, schema_name, include_public=True):
        """
        Main API method to current database schema,
        but it does not actually modify the db connection.
        """
        self.tenant = FakeTenant(schema_name=schema_name)
        self._set_schema(schema_name, include_public)

    def set_schema_to_public(self):
        """
        Instructs to stay in the common 'public' schema.
        """
        self.tenant = FakeTenant(schema_name=get_public_schema_name())
        self._set_schema(get_public_schema_name())

    def set_settings_schema(self, schema_name, include_public=True):
        self.settings_dict['SCHEMA'] = [schema_name]  # should not be getting set to public when not necessary
        if include_public and schema_name != get_public_schema_name():
            self.settings_dict['SCHEMA'].append(get_public_schema_name())

    def _set_schema(self, schema_name, include_public=True):
        self.schema_name = schema_name
        self.include_public_schema = include_public
        self.set_settings_schema(schema_name, include_public)
        self.search_path_set = False

    def get_schema(self):
        warnings.warn("connection.get_schema() is deprecated, use connection.schema_name instead.",
                      category=DeprecationWarning)
        return self.schema_name

    def get_tenant(self):
        warnings.warn("connection.get_tenant() is deprecated, use connection.tenant instead.",
                      category=DeprecationWarning)
        return self.tenant

    def _cursor(self, name=None):
        """
        Here it happens. We hope every Django db operation using PostgreSQL
        must go through this to get the cursor handle. We change the path.
        """
        if name:
            # Only supported and required by Django 1.11 (server-side cursor)
            cursor = super(DatabaseWrapper, self)._cursor(name=name)
        else:
            cursor = super(DatabaseWrapper, self)._cursor()

        # optionally limit the number of executions - under load, the execution
        # of `set search_path` can be quite time consuming
        if (not get_limit_set_calls()) or not self.search_path_set:
            # Actual search_path modification for the cursor. Database will
            # search schemata from left to right when looking for the object
            # (table, index, sequence, etc.).
            if not self.schema_name:
                raise ImproperlyConfigured("Database schema not set. Did you forget "
                                           "to call set_schema() or set_tenant()?")
            _check_schema_name(self.schema_name)
            public_schema_name = get_public_schema_name()

            if self.schema_name == public_schema_name:
                search_paths = [protect_case(public_schema_name)]
            elif self.include_public_schema:
                search_paths = [protect_case(self.schema_name), protect_case(public_schema_name)]
            else:
                search_paths = [protect_case(self.schema_name)]

            search_paths.extend([protect_case(extra_path) for extra_path in EXTRA_SEARCH_PATHS])

            if name:
                # Named cursor can only be used once
                cursor_for_search_path = self.connection.cursor()
            else:
                # Reuse
                cursor_for_search_path = cursor

            # In the event that an error already happened in this transaction and we are going
            # to rollback we should just ignore database error when setting the search_path
            # if the next instruction is not a rollback it will just fail also, so
            # we do not have to worry that it's not the good one
            try:
                cursor_for_search_path.execute('SET search_path = %s', (AsIs(','.join(search_paths)),))
            except (django.db.utils.DatabaseError, psycopg2.InternalError):
                self.search_path_set = False
            else:
                self.search_path_set = True
            if name:
                cursor_for_search_path.close()
        return cursor


class FakeTenant:
    """
    We can't import any db model in a backend (apparently?), so this class is used
    for wrapping schema names in a tenant-like structure.
    """
    def __init__(self, schema_name):
        self.schema_name = schema_name
