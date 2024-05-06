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
from requests_oauthlib import OAuth1
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from network_config import applovin, ironsource
from sign_generate import ironsource_key
import logging



def etl_fan_rev(today,yesterday):

    start_time = datetime.now()
    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    # APPLOVIN
    url = applovin.url_max
    
    # Input params
    params = applovin.params   
    params["columns"] = "day,country,platform,package_name,estimated_revenue,impressions,ecpm"
    params["start"] = start
    params["end"] = end
    params["filter_network"] = "FACEBOOK_NETWORK,FACEBOOK_NATIVE_BIDDING"
    
    # Make the GET request
    response = requests.get(url ,params=params)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()["results"]

        # Transform
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}
        dict_network = {network.name: network for network in network_set}
        game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "package_name"})

        for row in data:
            row["platform"] = row["platform"].lower().strip()
            row["package_name"] = row["package_name"].strip()
            row["country"] = row["country"].upper().strip()
            if row["country"] not in dict_country_id:
                row["country"] = 'OTH'
        
      
        applovin_df = pd.DataFrame(data)
        applovin_df = applovin_df[["day", "platform", "country", "package_name", "estimated_revenue", "impressions"]]
        applovin_df["estimated_revenue"]= applovin_df["estimated_revenue"].astype('float')
        applovin_df["impressions"]= applovin_df["impressions"].astype('int')
        print(f'Request and transform FAN From Applovin time: {datetime.now() - start_time}')

    # IRONSOURCE
    start_time = datetime.now()

    params = ironsource.params
    headers = ironsource.headers         
    url = ironsource.url_rev

    # Add params to requests
    params["startDate"] = start
    params["endDate"] = end
    params["breakdowns"] = "date,country,platform,app"
    params["metrics"] = "revenue,eCPM,impressions"
    params["adSource"] = "Meta"

    # Generate key
    key = ironsource_key(ironsource.secret_key, ironsource.refreshToken)
    headers["Authorization"] = "Bearer " + eval(key)

    # Make the GET request
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Transform
        # Create DataFrame
        df = pd.json_normalize(data,record_path='data',meta=['date', 'platform', 'bundleId'])
        df["bundleId"] = df["bundleId"].str.strip()
        df["countryCode"] = df["countryCode"].str.upper().str.strip().apply(lambda x: x if x in dict_country_id else 'OTH')
        df["platform"] = df["platform"].str.lower().str.strip()
        df.rename(
            columns={
                "date": "day", 
                "countryCode": "country", 
                "bundleId": "package_name", 
                "revenue": "estimated_revenue",
                }, 
            inplace=True
            )
        df = df[["day", "platform", "country", "package_name", "estimated_revenue", "impressions"]]
        
        print(f'Request and Transform FAN from Ironsource time: {datetime.now() - start_time}')
        start_time = datetime.now()
        df = pd.concat([applovin_df, df], ignore_index=True)
        df = df.merge(game_df, on =['platform', 'package_name'], how='left')
        df['package_name'] = np.where((df['platform'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['package_name'])
        df = df[df["package_name"].isin(dict_game_id)]
        df['estimated_revenue'] = df['estimated_revenue'].astype('float')
        df['impressions'] = df['impressions'].astype(int)
        df = df.groupby(["day", "platform", "country", "package_name"])[["estimated_revenue", "impressions"]].sum().reset_index()
        name_dict = {
            'day' : 'update_date', 
            'platform' : 'platform', 
            'country': 'country', 
            'package_name' : 'product_id',
            'estimated_revenue' : 'revenue',
            'impressions' : 'impressions'
            }
        df.rename(columns = name_dict, inplace  = True)
        df['ecpm'] = np.where(df['impressions'] == 0, 0, (df['revenue'] / df['impressions']) * 1000)
        df['network'] = 'FAN'
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/fan.csv',index = False)
        Transform_time = datetime.now() - start_time
        print(f'Transform FAN Success in: {Transform_time}')
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
    


def etl_fan_rev_nocountry(today,yesterday):
    start_time = datetime.now()
    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    # APPLOVIN
    url = applovin.url_max
    
    # Input params
    params = applovin.params   
    params["columns"] = "day,platform,package_name,impressions,estimated_revenue"
    params["start"] = start
    params["end"] = end
    params["filter_network"] = "FACEBOOK_NETWORK,FACEBOOK_NATIVE_BIDDING"
    
    # Make the GET request
    response = requests.get(url ,params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()["results"]

        # Transform
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_network = {network.name: network for network in network_set}
        game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "package_name"})

        for row in data:
            row["platform"] = row["platform"].lower().strip()
            row["package_name"] = row["package_name"].strip()
        
        
        applovin_df = pd.DataFrame(data)
        applovin_df = applovin_df[["day", "platform", "package_name", "estimated_revenue", "impressions"]]
        applovin_df["estimated_revenue"]= applovin_df["estimated_revenue"].astype('float')
        applovin_df["impressions"]= applovin_df["impressions"].astype('int')
        print(f'Request and transform FAN NOCOUNTRY From Applovin  in: {datetime.now() - start_time}')

    # IRONSOURCE
    start_time = datetime.now()

    params = ironsource.params
    headers = ironsource.headers         
    url = ironsource.url_rev

    # Add params to requests
    params["startDate"] = start
    params["endDate"] = end
    params["breakdowns"] = "date,platform,app"
    params["metrics"] = "revenue,eCPM,impressions"
    params["adSource"] = "Meta"

    # Generate key
    key = ironsource_key(ironsource.secret_key, ironsource.refreshToken)
    headers["Authorization"] = "Bearer " + eval(key)

    # Make the GET request
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Transform
        # Create DataFrame
        df = pd.json_normalize(data,record_path='data',meta=['date', 'platform', 'bundleId'])
        df["bundleId"] = df["bundleId"].str.strip()
        df["platform"] = df["platform"].str.lower().str.strip()
        df.rename(
            columns={
                "date": "day",  
                "bundleId": "package_name", 
                "revenue": "estimated_revenue",
                }, 
            inplace=True
            )
        df = df[["day", "platform", "package_name", "estimated_revenue", "impressions"]]
        
        print(f'Request and Transform FAN Nocountry From Ironsource time: {datetime.now() - start_time}')
        start_time = datetime.now()
        df = pd.concat([applovin_df, df], ignore_index=True)
        df = df.merge(game_df, on =['platform', 'package_name'], how='left')
        df['package_name'] = np.where((df['platform'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['package_name'])
        df = df[df["package_name"].isin(dict_game_id)]
        df['estimated_revenue'] = df['estimated_revenue'].astype('float')
        df['impressions'] = df['impressions'].astype("int")
        df = df.groupby(["day", "platform", "package_name"])[["estimated_revenue", "impressions"]].sum().reset_index()
        name_dict = {
            'day' : 'update_date', 
            'platform' : 'platform', 
            'package_name' : 'product_id',
            'estimated_revenue' : 'revenue',
            'impressions' : 'impressions'
            }
        df.rename(columns = name_dict, inplace  = True)
        df['ecpm'] = np.where(df['impressions'] == 0, 0, (df['revenue'] / df['impressions']) * 1000)
        df['network'] = 'FAN'
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/fan.csv',index = False)
        Transform_time = datetime.now() - start_time
        print(f'Transform FAN NOCOUNTRY Success in: {Transform_time}')
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
  


def run_fan():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_fan_rev(today,yesterday)
    etl_fan_rev_nocountry(today,yesterday)
