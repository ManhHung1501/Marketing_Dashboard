import sys
sys.path.insert(0, '/home/mkt-en/mkt_dashboard')
sys.path.insert(0, '/home/mkt-en/mkt_dashboard/dashboard/etl')
sys.path.insert(0, '/home/mkt-en/mkt_dashboard/dashboard/management/services')

import os
import django

# Set the environment variable for Django settings
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mkt_dashboard.settings")

# Initialize Django
django.setup()

# Continue with the rest of your code
from django.db import connection
import logging
from datetime import datetime, timedelta, date
from fyber import run_fyber
from telegram_service import send_table
import asyncio
import pandas as pd
import textwrap
from telegram import Bot
from telegram.ext import Updater



run_fyber()