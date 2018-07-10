from django_tenants.middleware.suspicious import SuspiciousTenantMiddleware
from django_tenants.utils import get_public_schema_name, get_tenant_model
from django.http import Http404


class DefaultTenantMiddleware(SuspiciousTenantMiddleware):
    """
    Extend the SuspiciousTenantMiddleware in scenario where you want to
    configure a tenant to be served if the hostname does not match any of the
    existing tenants.
    Subclass and override DEFAULT_SCHEMA_NAME to use a schema other than the
    public schema.
        class MyTenantMiddleware(DefaultTenantMiddleware):
            DEFAULT_SCHEMA_NAME = 'default'
    """
    DEFAULT_SCHEMA_NAME = None
    NO_TENANT_EXCEPTION = Http404  # in case no tenants exist yet

    def get_tenant(self, tenant_model, user):
        try:
            return super(DefaultTenantMiddleware, self).get_tenant(tenant_model, user)
        except self.TENANT_NOT_FOUND_EXCEPTION or self.NO_TENANT_EXCEPTION:
            schema_name = self.DEFAULT_SCHEMA_NAME
            if not schema_name:
                schema_name = get_public_schema_name()
            tenant_model = get_tenant_model()
            return tenant_model.objects.get(schema_name=schema_name)
        except Exception as e:
            print(e)
            print(e.message)
