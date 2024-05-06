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
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from network_config import ironsource
from sign_generate import ironsource_key
import logging
import json


def etl_ironsource_rev(today,yesterday):
    start_time = datetime.now()
    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    params = ironsource.params
    headers = ironsource.headers         
    url = ironsource.url_rev

    # Add params to requests
    params["startDate"] = start
    params["endDate"] = end
    params["breakdowns"] = "date,country,platform,app"
    params["metrics"] = "revenue,eCPM,impressions"
    params["adSource"] = "ironSource"

    # Generate key
    key = ironsource_key(ironsource.secret_key, ironsource.refreshToken)
    headers["Authorization"] = "Bearer " + eval(key)
    
    # Make the GET request
    response = requests.get(url, headers=headers, params=params)
    
    print(f"Request Ironsource Success in: {datetime.now() - start_time}")
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        start_time = datetime.now()
        # Transform
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}
        dict_network = {network.name: network for network in network_set}
        game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "bundleId"})

        # Create DataFrame
        df = pd.json_normalize(data,record_path='data',meta=['date', 'platform', 'bundleId'])
        df["platform"] = df["platform"].str.lower().str.strip()
        df["bundleId"] = df["bundleId"].str.strip()
        df = df.merge(game_df, on =['platform', 'bundleId'], how='left')
        df['bundleId'] = np.where((df['platform'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['bundleId'])
        df = df[df["bundleId"].isin(dict_game_id)]
        df["countryCode"] = df["countryCode"].str.upper().str.strip().apply(lambda x: x if x in dict_country_id else 'OTH')
        df["revenue"] = df["revenue"].astype("float")
        df['impressions'] = df["impressions"].astype(int)
        df = df.groupby(['date','platform','countryCode','bundleId'])[['revenue','impressions']].sum().reset_index()
        df['eCPM'] = np.where(df['impressions'] == 0, 0, (df['revenue'] / df['impressions']) * 1000)
        df['eCPM'] = df["eCPM"].astype("float")
        fill_values = {'impressions': 0, 'revenue': 0, 'eCPM': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[df["impressions"] != 0]
        name_dict = {
            'date' : 'update_date', 
            'platform' : 'platform', 
            'countryCode': 'country', 
            'bundleId' : 'product_id',
            'revenue' : 'revenue',
            'impressions' : 'impressions',
            'eCPM' : 'ecpm'
            }
        df.rename(columns = name_dict, inplace  = True)
        df["network"] = "Ironsource"
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]
        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/ironsource.csv',index = False)
        print(f"Transform Ironsource Success in: {datetime.now() - start_time}")
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)

def etl_ironsource_rev_nocountry(today,yesterday):
    start_time = datetime.now()
    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    params = ironsource.params
    headers = ironsource.headers         
    url = ironsource.url_rev

    # Add params to requests
    params["startDate"] = start
    params["endDate"] = end
    params["breakdowns"] = "date,platform,app"
    params["metrics"] = "revenue,eCPM,impressions"
    params["adSource"] = "ironSource"

    # Generate key
    key = ironsource_key(ironsource.secret_key, ironsource.refreshToken)
    headers["Authorization"] = "Bearer " + eval(key)
    
    # Make the GET request
    response = requests.get(url, headers=headers, params=params)
    
    print(f"Request Ironsource Success in: {datetime.now() - start_time}")
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        start_time = datetime.now()
        # Transform
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_network = {network.name: network for network in network_set}
        game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "bundleId"})

        # Create DataFrame
        df = pd.json_normalize(data,record_path='data',meta=['date', 'platform', 'bundleId'])
        df["platform"] = df["platform"].str.lower().str.strip()
        df["bundleId"] = df["bundleId"].str.strip()
        df = df.merge(game_df, on =['platform', 'bundleId'], how='left')
        df['bundleId'] = np.where((df['platform'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['bundleId'])
        df = df[df["bundleId"].isin(dict_game_id)]
        df["revenue"] = df["revenue"].astype("float")
        df["impressions"] = df["impressions"].astype(int)
        df = df.groupby(['date','platform','bundleId'])[['revenue','impressions']].sum().reset_index()
        df['eCPM'] = np.where(df['impressions'] == 0, 0, (df['revenue'] / df['impressions']) * 1000)
        fill_values = {'impressions': 0, 'revenue': 0, 'eCPM': 0}
        df.fillna(value=fill_values, inplace=True)

        name_dict = {
        'date' : 'update_date', 
        'platform' : 'platform', 
        'bundleId' : 'product_id',
        'revenue' : 'revenue',
        'impressions' : 'impressions',
        'eCPM' : 'ecpm'
        }
        df.rename(columns = name_dict, inplace  = True)
        df["network"] = "Ironsource"
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]
        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/ironsource.csv',index = False)
        print(f"Transform Ironsource NOCOUNTRY Success in: {datetime.now() - start_time}")


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
        if df['date'].nunique() == 2:
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


            print(f'Transform Cost Data with {len(df)} rows from Ironsrouce Success in {datetime.now() - now}')
            
            return df
        else:
            print(f"cost data from Ironsource on {end} is not ready")
            return pd.DataFrame()
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        return pd.DataFrame()

def run_ironsource():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_ironsource_rev(today,yesterday)
    etl_ironsource_rev_nocountry(today,yesterday)
