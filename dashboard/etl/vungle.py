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
from network_config import vungle
import logging
from sign_generate import PangleMediaUtil

def etl_vungle_rev(today, yesterday):
    start_time = datetime.now()

    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    headers = vungle.headers
    url = vungle.url_rev

    # Params
    params = vungle.params
    params["aggregates"] = "revenue,impressions,ecpm"
    params["dimensions"] = "date,country,platform,application"
    params["start"] = start
    params["end"] = end
    
    # Make the GET request
    response = requests.get(url, headers=headers ,params=params)

    print(f'Success REQUEST REVENUE Vungle in: {datetime.now() - start_time}')
    request_time = datetime.now()

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Transform
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}
        dict_network = {network.name: network for network in network_set}

        df = pd.DataFrame(data)
        df_app = pd.read_json("/home/mkt-en/mkt_dashboard/conf/app_vungle.json")
        df_app["bundle_id"] = df_app["store"].apply(lambda x: x["id"] if isinstance(x,dict) else x)
        df_app = df_app[["bundle_id","id"]]
        df = df.merge(df_app, left_on="application id", right_on="id", how="left")
        df["country"] = df["country"].str.upper().str.strip().apply(lambda x: x if x in dict_country_id else 'OTH')
        df["platform"] = df["platform"].str.lower().str.strip()
        df = df[df["bundle_id"].isin(dict_game_id)]
        df["revenue"] = df["revenue"].astype("float")
        df["impressions"] = df["impressions"].astype("int")

        df = df.groupby(['date','platform','country','bundle_id'])[['revenue','impressions']].sum().reset_index()
        name_dict = {
            'date' : 'update_date', 
            'platform' : 'platform',
            'country': 'country',  
            'bundle_id' : 'product_id',
            'revenue' : 'revenue',
            'impressions' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
        df["network"] = 'Vungle'
        
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/vungle.csv', index=False)

        print(f'Success TRANSFORM REVENUE Vungle in: {datetime.now() - request_time}')

    else:
        logging.info(f"Request data failed with status code: {response.status_code}")
        logging.info(response.text)



def etl_vungle_rev_nocountry(today, yesterday):
    start_time = datetime.now()
    
    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    headers = vungle.headers
    url = vungle.url_rev

    # Params
    params = vungle.params
    params["aggregates"] = "revenue,impressions,ecpm"
    params["dimensions"] = "date,platform,application"
    params["start"] = start
    params["end"] = end
    
    # Make the GET request
    response = requests.get(url, headers=headers ,params=params)

    print(f'Success REQUEST REVENUE Vungle NoCountry in: {datetime.now() - start_time}')
    request_time = datetime.now()

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Transform
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_network = {network.name: network for network in network_set}

        df = pd.DataFrame(data)
        df_app = pd.read_json("/home/mkt-en/mkt_dashboard/conf/app_vungle.json")
        df_app["bundle_id"] = df_app["store"].apply(lambda x: x["id"] if isinstance(x,dict) else x)
        df_app = df_app[["bundle_id","id"]]
        df = df.merge(df_app, left_on="application id", right_on="id", how="left")
        df["platform"] = df["platform"].str.lower().str.strip()
        df = df[df["bundle_id"].isin(dict_game_id)]
        df["revenue"] = df["revenue"].astype("float")
        df["impressions"] = df["impressions"].astype("int")

        df = df.groupby(['date','platform','bundle_id'])[['revenue','impressions']].sum().reset_index()
        df = df[df["impressions"] != 0]
        name_dict = {
            'date' : 'update_date', 
            'platform' : 'platform', 
            'bundle_id' : 'product_id',
            'revenue' : 'revenue',
            'impressions' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
        df["network"] = 'Vungle'
        
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/vungle.csv', index=False)

        print(f'Success TRANSFORM REVENUE Vungle NoCountry in: {datetime.now() - request_time}')

    else:
        logging.info(f"Request data failed with status code: {response.status_code}")
        logging.info(response.text)
    

def etl_vungle_cost(today,yesterday):
    now = datetime.now()

    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    headers = vungle.headers
    url = vungle.url_cost
    
    # Params
    params = vungle.params
    params["aggregates"] = "spend,eCPI,installs"
    params["dimensions"] = "date,country,platform,application"
    params["start"] = start
    params["end"] = end

    # Make the GET request
    response = requests.get(url, headers=headers ,params=params, timeout=600)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        print(f'Request Cost Data with {len(data)} rows from Vungle Success in {datetime.now() - now}')
        now = datetime.now()

        # Transform
        game_set = Game.objects.all()
        country_set = Country.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}
        dict_network = {network.name: network for network in network_set}
        game_df = pd.DataFrame(game_set.values())
        
        df = pd.DataFrame(data)
        if len(df) > 0:
            df_app = pd.read_json("/home/mkt-en/mkt_dashboard/conf/app_vungle.json")
            df_app["bundle_id"] = df_app["store"].apply(lambda x: x["id"] if isinstance(x,dict) else x)
            df_app = df_app[["bundle_id","id"]]
            
            update_app = [
                {"bundle_id":"1176011642", "id": "5ab9cc65b0adab36053c91f0"},
                {"bundle_id":"com.alien.shooter.galaxy.attack", "id": "5aa8ec034ed761881e00ee88"},
            ]
            update_app = pd.DataFrame(update_app)
            df_app = pd.concat([df_app, update_app], ignore_index=True)
            
            df = df.merge(df_app, left_on="application id", right_on="id", how="left")
            df["platform"] = df["platform"].str.lower().str.strip()
            df["country"] = df["country"].str.upper().str.strip().apply(lambda x: x if x in dict_country_id else 'OTH')
            df = df[df["bundle_id"].isin(dict_game_id)]
            df['network'] = 'Vungle'

            if df['date'].nunique() == 2:
                df = df.groupby(['date','country','bundle_id','network'])[['spend','installs']].sum().reset_index()
                name_dict = {
                    'date' : 'update_date', 
                    'country': 'country', 
                    'bundle_id' : 'product_id',
                    'spend' : 'cost',
                    'installs' : 'installs'
                    }
                df.rename(columns=name_dict, inplace=True)

                # Fill nan
                fill_values = {'installs': 0, 'cost': 0}
                df.fillna(value=fill_values, inplace=True)

                print(f'Transform Cost Data with {len(df)} rows from Vungle Success in {datetime.now() - now}')
                return df
            else:
                print(f"cost data from Vungle on {end} is not ready")
                return pd.DataFrame()
        else:
            print(f"Have No Cost Data from Vungle")
            return pd.DataFrame()
    else:
        print(f"Request data failed with status code: {response.status_code}")
        print(response.text)

def run_vungle():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_vungle_rev(today,yesterday)
    etl_vungle_rev_nocountry(today,yesterday)

