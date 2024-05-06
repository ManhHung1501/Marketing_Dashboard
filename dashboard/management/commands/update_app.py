from django.core.management.base import BaseCommand
from dashboard.management.services import get_app_details, get_app_vungle, exchange
from datetime import date, datetime, timedelta, time as datetime_time
import sys

class Command(BaseCommand):
    help = 'Update App'
    def add_arguments(self, parser):
        # Positional arguments
        parser.add_argument("-vungle", nargs="?", type=str)

    def handle(self, *args, **kwargs):
        sys.stdout.write("Start Update App")
        get_app_details.update_app()
        sys.stdout.write("Success Update App")
        exchange.exchange_rate(datetime.now())
        if kwargs["vungle"]:
            get_app_vungle.request_app(kwargs["vungle"])
            sys.stdout.write("Success Update App Vungle")