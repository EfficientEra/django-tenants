from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db import connection
from django.http import Http404


from django.utils.deprecation import MiddlewareMixin

from django_tenants.utils import get_public_schema_name, get_tenant_model


class TenantMainMiddleware(MiddlewareMixin):
    TENANT_NOT_FOUND_EXCEPTION = Http404
    """
    This middleware should be placed at the very top of the middleware stack.
    Selects the proper database schema using the request host. Can fail in
    various ways which is better than corrupting or revealing data.
    """

    @staticmethod
    def get_tenant(tenant_model, user):
        if not user:
            raise tenant_model.DoesNotExist('No user logged in')
        if user.is_authenticated() and not user.is_staff:
            return tenant_model.objects.get(user=user)

    def process_request(self, request):
        # Connection needs first to be at the public schema, as this is where
        # the tenant metadata is stored.
        connection.set_schema_to_public()
        tenant_model = get_tenant_model()
        user = getattr(request, 'user', None)

        try:
            tenant = self.get_tenant(tenant_model, user)
        except tenant_model.DoesNotExist:
            raise self.TENANT_NOT_FOUND_EXCEPTION('No tenant for user "{}"'.format(user))

        request.tenant = tenant

        connection.set_tenant(request.tenant)

        # Content type can no longer be cached as public and tenant schemas
        # have different models. If someone wants to change this, the cache
        # needs to be separated between public and shared schemas. If this
        # cache isn't cleared, this can cause permission problems. For example,
        # on public, a particular model has id 14, but on the tenants it has
        # the id 15. if 14 is cached instead of 15, the permissions for the
        # wrong model will be fetched.
        ContentType.objects.clear_cache()

        # Do we have a public-specific urlconf?
        if hasattr(settings, 'PUBLIC_SCHEMA_URLCONF') and request.tenant.schema_name == get_public_schema_name():
            request.urlconf = settings.PUBLIC_SCHEMA_URLCONF
