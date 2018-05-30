from django.core import exceptions
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils.encoding import force_str
from django.utils.six.moves import input
from django.db.utils import IntegrityError
from django_tenants.utils import get_tenant_model


class Command(BaseCommand):
    help = 'Create a tenant'

    # Only use editable fields
    tenant_fields = [field for field in get_tenant_model()._meta.fields
                     if field.editable and not field.primary_key]

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)

    def add_arguments(self, parser):

        for field in self.tenant_fields:
            parser.add_argument('--%s' % field.name, help='Specifies the %s for tenant.' % field.name)

        parser.add_argument('-s', action="store_true",
                                         help='Create a superuser afterwards.')

    def handle(self, *args, **options):

        tenant_data = {}
        tenant = BaseCommand
        for field in self.tenant_fields:
            input_value = options.get(field.name, None)
            tenant_data[field.name] = input_value

        while True:
            for field in self.tenant_fields:
                if tenant_data.get(field.name, '') == '':
                    input_msg = field.verbose_name
                    default = field.get_default()
                    if default:
                        input_msg = "%s (leave blank to use '%s')" % (input_msg, default)

                    input_value = input(force_str('%s: ' % input_msg)) or default
                    tenant_data[field.name] = input_value
            tenant = self.store_tenant(**tenant_data)
            if tenant is not None:
                break
            tenant_data = {}

        if options.get('s', None):
            self.stdout.write("Create superuser for %s" % tenant_data['schema_name'])
            call_command('create_tenant_superuser', schema_name=tenant_data['schema_name'], interactive=True)

    def store_tenant(self, **fields):
        try:
            tenant = get_tenant_model().objects.create(**fields)
            return tenant
        except exceptions.ValidationError as e:
            self.stderr.write("Error: %s" % '; '.join(e.messages))
            return None
        except IntegrityError:
            return None
