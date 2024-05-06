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
from datetime import datetime, timedelta,date
from network_config import applovin
import logging

def etl_applovin_cost(today,yesterday):

    now = datetime.now()

    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    url = applovin.url
    
    # Input params for requests
    params = applovin.params 
    params["report_type"] = "advertiser"
    params["columns"] = "day,platform,country,campaign_package_name,cost,installs,average_cpa"
    params["start"] = start
    params["end"] = end
    params["having"] = "installs > 0 OR cost > 0"
    

    # Make the GET request
    response = requests.get(url ,params=params)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()["results"]
        
        print(f'Request Applovin Cost with {len(data)} rows Success in: {datetime.now() - now}')
        now = datetime.now()

    # Transform
        game_set = Game.objects.all()
        country_set = Country.objects.all()
        network_set = Network.objects.all()
        dict_country_id = {country.country_id: country  for country in country_set}
        dict_game_id = {game.id_bundle.lower(): game for game in game_set}
        dict_network = {network.name: network for network in network_set}
        game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "package_name"})
        

        for row in data:
            row["platform"] = row["platform"].lower().strip()
            row["package_name"] = row["campaign_package_name"].strip().lower()
            row["country"] = row["country"].upper().strip()
            if  row["country"] not in dict_country_id:
                row["country"] = 'OTH'
            

        df = pd.DataFrame(data)
        df = df.merge(game_df, on=["platform", "package_name"], how="left")
        df['package_name'] = np.where((df['platform'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['package_name'])
        df = df[((df['platform'] == 'ios') & (df['id_bundle'].notna()) | (df["platform"] == 'android'))]
        df = df[df["package_name"].isin(dict_game_id)]

        df["cost"] = df["cost"].astype("float")
        df["installs"] = df["installs"].astype("int")

        # Groupby df
        df["network"] = "Applovin"

        if df["day"].nunique() == 2:
            df = df.groupby(['day','country','package_name','network'])[['cost','installs']].sum().reset_index()
            name_dict = {
                'day' : 'update_date', 
                'country': 'country', 
                'package_name' : 'product_id',
                'cost' : 'cost',
                'installs' : 'installs'
                }
            df.rename(columns=name_dict, inplace=True)

            # Fill nan
            fill_values = {'installs': 0, 'cost': 0}
            df.fillna(value=fill_values, inplace=True)

            print(f'Transform Applovin Cost with {len(df)} rows Success in: {datetime.now() - now}')

            return df
        
        else:
            print(f"Cost Data from Applovin of {end} is not ready")
            return pd.DataFrame()

    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)


def etl_applovin_rev(today,yesterday):
    start_time = datetime.now()
    
    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    url = applovin.url
    
    # Input params
    params = applovin.params   
    params["report_type"] = "publisher"
    params["columns"] = "day,platform,country,package_name,revenue,impressions,ecpm"
    params["start"] = start
    params["end"] = end
    params["having"] = "impressions > 0 OR revenue > 0"
    
    # Make the GET request
    response = requests.get(url ,params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()["results"]
        print(f'Success REQUEST REVENUE Applovin in: {datetime.now() - start_time}')
        request_time = datetime.now()

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
            if row["package_name"] == 'com.megajoy.prisonbreak68':
                row["package_name"] ='com.MegaJoy.PrisonBreak68'
            row["country"] = row["country"].upper().strip()
            if row["country"] not in dict_country_id:
                row["country"] = 'OTH'
  
        df = pd.DataFrame(data)
        df = df.merge(game_df, on=["platform", "package_name"], how="left")
        df['package_name'] = np.where((df['platform'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['package_name'])
        df = df[((df['platform'] == 'ios') & (df['id_bundle'].notna()) | (df["platform"] == 'android'))]
        df = df[df["package_name"].isin(dict_game_id)]
        df['revenue'] = df['revenue'].astype('float')
        df['impressions'] = df['impressions'].astype('int')
        df = df.groupby(['day','platform','country','package_name'])[['revenue','impressions']].sum().reset_index()
        name_dict = {
            'day' : 'update_date', 
            'platform' : 'platform', 
            'country': 'country', 
            'package_name' : 'product_id',
            'revenue' : 'revenue',
            'impressions' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
        df["network"] = 'Applovin'
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/applovin.csv', index=False)

        print(f'Success TRANSFORM REVENUE Applovin in: {datetime.now() - request_time}')
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)


def etl_applovin_rev_nocountry(today,yesterday):
    try:
        start_time = datetime.now()
        
        start = yesterday.strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
        
        url = applovin.url
        
        # Input params
        params = applovin.params   
        params["report_type"] = "publisher"
        params["columns"] = "day,platform,package_name,revenue,impressions,ecpm"
        params["start"] = start
        params["end"] = end
        params["having"] = "impressions > 0 OR revenue > 0"
        
        # Make the GET request
        response = requests.get(url ,params=params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()["results"]

            
            print(f'Success REQUEST REVENUE NOCOUNTRY Applovin in: {datetime.now() - start_time}')
            request_time = datetime.now()

            # Transform
            game_set = Game.objects.all()
            network_set = Network.objects.all()
            dict_game_id = {game.id_bundle: game for game in game_set}
            dict_network = {network.name: network for network in network_set}
            game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "package_name"})
            
            for row in data:
                row["platform"] = row["platform"].lower().strip()
                row["package_name"] = row["package_name"].strip()
                if row["package_name"] == 'com.megajoy.prisonbreak68':
                    row["package_name"] = 'com.MegaJoy.PrisonBreak68'
                    
                
            df = pd.DataFrame(data)
            df = df.merge(game_df, on=["platform", "package_name"], how="left")
            df['package_name'] = np.where((df['platform'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['package_name'])
            df = df[df["package_name"].isin(dict_game_id)]
            df['revenue'] = df['revenue'].astype('float')
            df['impressions'] = df['impressions'].astype('int')
            df = df.groupby(['day','platform','package_name'])[['revenue','impressions']].sum().reset_index()
            name_dict = {
                'day' : 'update_date', 
                'platform' : 'platform',  
                'package_name' : 'product_id',
                'revenue' : 'revenue',
                'impressions' : 'impressions'
                }
            df.rename(columns=name_dict, inplace=True)
            df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
            df["network"] = 'Applovin'

            fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
            df.fillna(value=fill_values, inplace=True)

            df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/applovin.csv', index=False)
            
            print(f'Success TRANSFORM REVENUE NOCOUNTRY Applovin in: {datetime.now() - request_time}')

        else:
            print(f"Request failed with status code: {response.status_code}")
            print(response.text)
    except Exception as e:
        logging.error(e)

def run_applovin():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_applovin_rev(today,yesterday)
    etl_applovin_rev_nocountry(today,yesterday)



