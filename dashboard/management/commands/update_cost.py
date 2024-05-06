from django.core.management.base import BaseCommand
from dashboard.management.services import appsflyer
from datetime import date, datetime, timedelta, time as datetime_time
import sys

class Command(BaseCommand):
    help = 'Update Cost from Appsflyer'
    def handle(self, *args, **kwargs):
        appsflyer.load_cost(date.today() - timedelta(days = 12), date.today() - timedelta(days = 8))
        sys.stdout.write("Success Update Appsflyer")
            