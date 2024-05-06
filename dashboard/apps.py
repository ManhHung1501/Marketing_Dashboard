from django.apps import AppConfig
from architect.commands import partition
from django.db import ProgrammingError
from django.db.models.signals import post_migrate

class DashboardConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'dashboard'
    def ready(self):
        super(DashboardConfig, self).ready()
        # Hook up Architect to the post migrations signal
        post_migrate.connect(create_partitions, sender=self)

def create_partitions(sender, **kwargs):
    """
    After running migrations, go through each of the models
    in the app and ensure the partitions have been setup
    """
    paths = {model.__module__ for model in sender.get_models()}
    for path in paths:
        try:
            partition.run(dict(module=path))
        except ProgrammingError:
            # Possibly because models were just un-migrated or
            # fields have been changed that effect Architect
            print("Unable to apply partitions for module '{}'".format(path))
        else:
            print("Applied partitions for module '{}'".format(path))
