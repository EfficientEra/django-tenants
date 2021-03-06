from django.db import models, connections
from django.core.management import call_command
# noinspection PyProtectedMember
from psycopg2.extensions import AsIs
from .postgresql_backend.base import _check_schema_name
from .signals import post_schema_sync, schema_needs_to_be_sync, post_schema_migrate
from .utils import get_public_schema_name, get_creation_fakes_migrations, get_tenant_database_alias, schema_exists, clone_schema, get_tenant_base_schema


class TenantMixin(models.Model):
    """
    All tenant models must inherit this class.
    """

    auto_drop_schema = False
    """
    USE THIS WITH CAUTION!
    Set this flag to true on a parent class if you want the schema to be
    automatically deleted if the tenant row gets deleted.
    """

    auto_create_schema = True
    """
    Set this flag to false on a parent class if you don't want the schema
    to be automatically created upon save.
    """

    schema_name = models.CharField(max_length=63, unique=True,
                                   validators=[_check_schema_name])

    class Meta:
        abstract = True

    def __enter__(self):
        """
        Syntax sugar which helps in celery tasks, cron jobs, and other scripts

        Usage:
            with Tenant.objects.get(schema_name='test') as tenant:
                # run some code in tenant test
            # run some code in previous tenant (public probably)
        """
        connection = connections[get_tenant_database_alias()]
        self._previous_tenant = connection.tenant
        self.activate()

    def __exit__(self, exc_type, exc_val, exc_tb):
        connection = connections[get_tenant_database_alias()]

        connection.set_tenant(self._previous_tenant)

    def activate(self):
        """
        Syntax sugar that helps at django shell with fast tenant changing

        Usage:
            Tenant.objects.get(schema_name='test').activate()
        """
        connection = connections[get_tenant_database_alias()]
        connection.set_tenant(self)

    @classmethod
    def deactivate(cls):
        """
        Syntax sugar, return to public schema

        Usage:
            test_tenant.deactivate()
            # or simpler
            Tenant.deactivate()
        """
        connection = connections[get_tenant_database_alias()]
        connection.set_schema_to_public()

    def save(self, verbosity=1, *args, **kwargs):
        connection = connections[get_tenant_database_alias()]
        is_new = self.pk is None
        has_schema = hasattr(connection, 'schema_name')
        if has_schema and is_new and connection.schema_name != get_public_schema_name():
            raise Exception("Can't create tenant outside the public schema. "
                            "Current schema is %s." % connection.schema_name)
        elif has_schema and not is_new and connection.schema_name not in (self.schema_name, get_public_schema_name()):
            raise Exception("Can't update tenant outside it's own schema or "
                            "the public schema. Current schema is %s."
                            % connection.schema_name)

        super(TenantMixin, self).save(*args, **kwargs)

        if has_schema and is_new and self.auto_create_schema:
            try:
                self.create_schema(check_if_exists=True, verbosity=verbosity)
                post_schema_sync.send(sender=TenantMixin, tenant=self.serializable_fields())
            except Exception:
                # We failed creating the tenant, delete what we created and
                # re-raise the exception
                self.delete(force_drop=True)
                raise
        elif is_new:
            # although we are not using the schema functions directly, the signal might be registered by a listener
            schema_needs_to_be_sync.send(sender=TenantMixin, tenant=self.serializable_fields())
        elif not schema_exists(self.schema_name) and self.auto_create_schema and not is_new:
            # Create schemas for existing models, deleting only the schema on failure
            try:
                self.create_schema(check_if_exists=True, verbosity=verbosity)
                post_schema_sync.send(sender=TenantMixin, tenant=self.serializable_fields())
            except Exception:
                # We failed creating the schema, delete what we created and
                # re-raise the exception
                self.delete_schema()
                self.schema_name = None
                raise

    def serializable_fields(self):
        """ in certain cases the user model isn't serializable so you may want to only send the id """
        return self

    def delete(self, force_drop=False, *args, **kwargs):
        """
        Deletes this row. Drops the tenant's schema if the attribute
        auto_drop_schema set to True.
        """
        do_delete_schema = self.auto_drop_schema or force_drop
        self.delete_schema(do_delete_schema)

        super(TenantMixin, self).delete(*args, **kwargs)

    def delete_schema(self, allow_delete=True):
        """
        Drop the tenant's associated schema.
        """
        connection = connections[get_tenant_database_alias()]
        has_schema = hasattr(connection, 'schema_name')
        if has_schema and connection.schema_name not in (self.schema_name, get_public_schema_name()):
            raise Exception("Can't delete tenant outside it's own schema or "
                            "the public schema. Current schema is %s."
                            % connection.schema_name)

        if has_schema and schema_exists(self.schema_name) and allow_delete:
            connection.set_schema(self.schema_name, include_public=True)
            cursor = connection.cursor()
            cursor.execute('DROP SCHEMA %s CASCADE', (AsIs(connection.ops.quote_name(self.schema_name)),))

    def create_schema(self, check_if_exists=False, sync_schema=True,
                      verbosity=1):
        """
        Creates the schema 'schema_name' for this tenant. Optionally checks if
        the schema already exists before creating it. Returns true if the
        schema was created, false otherwise.
        """

        # safety check
        connection = connections[get_tenant_database_alias()]
        _check_schema_name(self.schema_name)
        cursor = connection.cursor()

        if check_if_exists and schema_exists(self.schema_name):
            return False

        fake_migrations = get_creation_fakes_migrations()

        if sync_schema:
            try:
                if fake_migrations:
                    # copy tables and data from provided model schema
                    base_schema = get_tenant_base_schema()
                    clone_schema(base_schema, self.schema_name)
                    post_schema_migrate.send(sender=TenantMixin, tenant=self.serializable_fields())
                else:
                    # create the schema
                    cursor.execute('CREATE SCHEMA %s', (AsIs(connection.ops.quote_name(self.schema_name)),))
                    call_command('migrate_schemas',
                                 tenant=True,
                                 schema_name=self.schema_name,
                                 interactive=False,
                                 verbosity=verbosity)
            except Exception:
                self.delete_schema()
                raise

        connection.set_schema_to_public()
