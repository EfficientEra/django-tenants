# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

from django.conf import settings
from django.db import connection
from django_tenants.middleware.default import DefaultTenantMiddleware
from django.contrib.contenttypes.models import ContentType
from django_tenants.utils import get_public_schema_name, get_tenant_model
from django.http import Http404


class CompatTenantMiddleware(DefaultTenantMiddleware):
    """
    Extend the DefaultTenantMiddleware in scenario where you want to
    allow existing users to access the app without having a tenant set up.
    """
    DEFAULT_SCHEMA_NAME = None
    NO_TENANT_EXCEPTION = Http404  # in case no tenants exist yet

    def process_request(self, request):
        # Connection needs first to be at the public schema, as this is where
        # the tenant metadata is stored.
        connection.set_schema_to_public()
        tenant_model = get_tenant_model()
        user = getattr(request, 'user', None)

        try:
            tenant = self.get_tenant(tenant_model, user)
        except tenant_model.DoesNotExist:
            tenant = tenant_model.objects.get(schema_name=get_public_schema_name())

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