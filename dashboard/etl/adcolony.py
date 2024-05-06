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
from datetime import datetime, timedelta
from network_config import adcolony
import logging
import numpy as np


def etl_adcolony_rev(today,yesterday):
    start_time = datetime.now()
    start = yesterday.strftime("%m%d%Y")
    end = today.strftime("%m%d%Y")
    
    url = adcolony.url_rev

    # Input params
    params = adcolony.params   
    params["date_group"] = 'day'
    params["group_by"] = "app,country"
    params["date"] = start
    params["end_date"] = end
    
    response = requests.get(url ,params=params)
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()["results"]
        request_time = datetime.now() - start_time
        print(f'Request Adcolony Success in: {request_time}')
        request_time = datetime.now()
        
        # Transform
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}
        dict_network = {network.name: network for network in network_set}
        if len(data) > 0:
            for row in data:
                row["platform"] = row["platform"].lower().strip()
                row["store_id"] = row["store_id"].strip()
                row["country"] = row["country"].upper().strip()
                if row["country"] not in dict_country_id:
                    row["country"] = 'OTH'

            filtered_data = [row for row in data if row["store_id"] in dict_game_id]
            df = pd.DataFrame(filtered_data)
            df['earnings'] = df['earnings'].astype('float')
            df['impressions'] = df['impressions'].astype('int')
            df = df.groupby(['date','platform','country','store_id'])[['earnings','impressions']].sum().reset_index()
            name_dict = {
                'date' : 'update_date', 
                'platform' : 'platform', 
                'country': 'country', 
                'store_id' : 'product_id',
                'earnings' : 'revenue',
                'impressions' : 'impressions'
                }
            df.rename(columns = name_dict, inplace  = True)
            df['ecpm'] = np.where(df['impressions'] == 0, 0, (df['revenue'] / df['impressions']) * 1000)
            df["network"] = "AdColony"
            fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
            df.fillna(value=fill_values, inplace=True)
            df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]
            df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/adcolony.csv',index = False)

            Transform_time = datetime.now() - request_time
            print(f'Transform Adcolony Success in: {Transform_time}')
        else:
            return pd.DataFrame()
    else:
        return pd.DataFrame()
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)

def etl_adcolony_rev_nocountry(today,yesterday):
    start_time = datetime.now()
    start = yesterday.strftime("%m%d%Y")
    end = today.strftime("%m%d%Y")
    
    url = adcolony.url_rev

    # Input params
    params = adcolony.params   
    params["date_group"] = 'day'
    params["group_by"] = "app"
    params["date"] = start
    params["end_date"] = end
    
    response = requests.get(url ,params=params)
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()["results"]
        request_time = datetime.now() - start_time
        print(f'Request Adcolony NOCOUNTRY Success in: {request_time}')
        request_time = datetime.now()
        
        # Transform
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_network = {network.name: network for network in network_set}
        if len(data) > 0:
            for row in data:
                row["platform"] = row["platform"].lower().strip()
                row["store_id"] = row["store_id"].strip()

            filtered_data = [row for row in data if row["store_id"] in dict_game_id]
            df = pd.DataFrame(filtered_data)
            df = df.groupby(['date','platform','store_id'])[['earnings','impressions']].sum().reset_index()
            name_dict = {
                'date' : 'update_date', 
                'platform' : 'platform', 
                'store_id' : 'product_id',
                'earnings' : 'revenue',
                'impressions' : 'impressions'
                }
            df.rename(columns = name_dict, inplace  = True)
            df['ecpm'] = np.where(df['impressions'] == 0, 0, (df['revenue'] / df['impressions']) * 1000)
            df["network"] = "AdColony"
            fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
            df.fillna(value=fill_values, inplace=True)
            df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]
            df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/adcolony.csv',index = False)

            Transform_time = datetime.now() - request_time
            print(f'Transform Adcolony NOCOUNTRY Success in: {Transform_time}')
        else:
            return pd.DataFrame()
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        return pd.DataFrame()

def etl_adcolony_cost(today,yesterday):
    now = datetime.now()

    start = yesterday.strftime("%m%d%Y")
    end = today.strftime("%m%d%Y")
    
    url = adcolony.url_cost

    # Input params
    params = adcolony.params   
    params["date_group"] = 'day'
    params["date"] = start
    params["end_date"] = end
    
    response = requests.get(url ,params=params)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()["results"]

        print(f' REquest AdColony Cost Success in {datetime.now() - now}')
        now = datetime.now()
        
        # # Transform
        if len(data) > 0:
            country_set = Country.objects.all()
            game_set = Game.objects.all()
            network_set = Network.objects.all()
            dict_game_id = {game.id_bundle: game for game in game_set}
            dict_country_id = {country.country_id: country for country in country_set}
            dict_network = {network.name: network for network in network_set}
            game_df = pd.DataFrame(game_set.values())

            for row in data:
                row["platform"] = row["platform"].lower().strip()
                row["store_id"] = row["store_id"].strip()
                row["country"] = row["country"].upper().strip()
                if row["country"] not in dict_country_id:
                    row["country"] = 'OTH'
                    
                
            filtered_data = [row for row in data if row["store_id"] in dict_game_id]
            df = pd.DataFrame(filtered_data)
            df["spend"] = df["spend"].astype("float")
            df["installs"] = df["installs"].astype("int")
            df["network"] = "AdColony"
            
            if df["date"].nunique() == 2:
                df = df.groupby(['date','country','store_id','network'])[['spend','installs']].sum().reset_index()
                name_dict = {
                    'date' : 'update_date', 
                    'country': 'country', 
                    'store_id' : 'product_id',
                    'spend' : 'cost',
                    'installs' : 'installs'
                    }
                df.rename(columns=name_dict, inplace=True)

                # Fill nan
                fill_values = {'installs': 0, 'cost': 0}
                df.fillna(value=fill_values, inplace=True)

                print(f'Transform AdColony Cost with {len(df)} rows Success in: {datetime.now() - now}')
                return df
                
            else:
                print(f"Cost Data from AdColony of {end} is not ready")
                return pd.DataFrame()
          
        else:
            print(f'Have no data')
            return pd.DataFrame()
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        return pd.DataFrame()

def run_adcolony():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_adcolony_rev(today,yesterday)
    etl_adcolony_rev_nocountry(today,yesterday)
