from optparse import make_option

from django.conf import settings
from django.core.cache import caches
from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS


class Command(BaseCommand):
    help = "Creates the tables needed to use the SQL cache backend."

    option_list = BaseCommand.option_list + (
        make_option('--database', action='store', dest='database',
            default=DEFAULT_DB_ALIAS, help='Nominates a database onto '
                'which the cache tables will be installed. '
                'Defaults to the "default" database.'),
    )

    requires_system_checks = False

    def handle(self, *tablenames, **options):
        db = options.get('database')
        self.verbosity = int(options.get('verbosity'))
        if len(tablenames):
            # Legacy behavior, tablename specified as argument
            for tablename in tablenames:
                self.create_table(db, tablename)
        else:
            for cache_alias in settings.CACHES:
                cache = caches[cache_alias]
