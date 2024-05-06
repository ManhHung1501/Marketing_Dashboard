import sys
sys.path.insert(0, '/home/mkt-en/mkt_dashboard')
import os
import django
# Configure Django settings
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mkt_dashboard.settings")
django.setup()
from django.db import connection
from mkt_dashboard.settings import DATABASES
from dashboard.models import Game,Country,Network
from googleapiclient import discovery
from googleapiclient.http import build_http
from apiclient.discovery import build
from googleapiclient.errors import HttpError
import sys
from datetime import date
from datetime import timedelta
import google.oauth2.credentials
import csv
import json
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta, time as datetime_time
from google.auth.transport.requests import Request
import socket
from google.auth import exceptions
from network_config import ironsource
from sign_generate import ironsource_key
import requests

def etl_ironsource_cost(today,yesterday):
    now = datetime.now()

    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    url = ironsource.url_cost
    headers = ironsource.headers
    
    # Add params to requests
    params = ironsource.params
    params["breakdowns"] = "day,title,os,country"
    params["metrics"] = "billable_spend,installs,ecpi"
    params["count"] = 50000
    params["startDate"] = start
    params["endDate"] = end

    # Add key
    key = ironsource_key(ironsource.secret_key, ironsource.refreshToken)
    headers["Authorization"] = "Bearer " + eval(key)
    
    # Make the GET request
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Transform data
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        country_set = Country.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_network = {network.name: network for network in network_set}
        dict_country_id = {country.country_id: country for country in country_set}
        game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "titleBundleId", "platform": "os"})

        result_data = data["data"]
        while data.get("paging") is not None:
            url = data["paging"]["next"]
            response = requests.get(url, headers=headers)
            data = response.json()
            result_data.extend(data["data"])

        print(f'Request Cost Data with {len(result_data)} rows from Ironsrouce Success in {datetime.now() - now}')
        now = datetime.now()

        # Transform
        df = pd.DataFrame(result_data)
        df['country'] = df['country'].str.upper().str.strip().apply(lambda x: x if x in dict_country_id else 'OTH')
        df["os"] = df["os"].str.lower().str.strip()
        df["titleBundleId"] = df["titleBundleId"].str.strip()
        df = df.merge(game_df, on =['os', 'titleBundleId'], how='left')
        df['titleBundleId'] = np.where((df['os'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['titleBundleId'])
        df = df[((df['os'] == 'ios') & (df['id_bundle'].notna()) | (df["os"] == 'android'))]
        df = df[df["titleBundleId"].isin(dict_game_id)]
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%dT%H:%M:%S.%fZ').dt.date
        df['network'] = 'Ironsource'

        # Group by
        
        df = df.groupby(['date','country','titleBundleId','network'])[['billableSpend','installs']].sum().reset_index()
        name_dict = {
            'date' : 'update_date', 
            'country': 'country', 
            'titleBundleId' : 'product_id',
            'billableSpend' : 'cost',
            'installs' : 'installs'
            }
        df.rename(columns=name_dict, inplace=True)

        # Fill nan
        fill_values = {'installs': 0, 'cost': 0}
        df.fillna(value=fill_values, inplace=True)

        game = 'com.alien.shooter.galaxy.attack'

        df = df[df["product_id"]==game]
        print(df['cost'].sum())

        print(f'Transform Cost Data with {len(df)} rows from Ironsrouce Success in {datetime.now() - now}')
        
        return df
      
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)


today = datetime.now() - timedelta(days=1)
etl_ironsource_cost(today,today)