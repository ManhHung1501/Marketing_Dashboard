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
from network_config import unity
import logging
import requests
from requests.auth import HTTPBasicAuth
import time
import pandas as pd
from io import StringIO
import numpy as np
from datetime import datetime, timedelta,date

def get_unity_bundleid():
    url = f"https://services.api.unity.com/monetize/v1/organizations/{unity.organize_id}/projects"
    response = requests.get(url, auth=HTTPBasicAuth(unity.refresh_token, unity.secretkey ))
    data = response.json()
    unity_data =[]
    for item in data:
        if item["stores"]["apple"].get("storeId") is not None:
            unity_data.append(    
                {
                    'gameId': item['stores']['apple']['gameId'],
                    'storeId': item['stores']['apple']['storeId']
                }
            )
        if item["stores"]["google"].get("storeId") is not None:
            unity_data.append(    
                {
                    'gameId': item['stores']['google']['gameId'],
                    'storeId': item['stores']['google']['storeId']
                }
            )        
    
    return pd.DataFrame(unity_data)

def unity_bundleid_cost():
    url = f"https://services.api.unity.com/advertise/v1/organizations/{unity.organize_id}/apps"
    offset = 0
    params = {
        "offset": offset,
        "limit": 1000
    }
    response = requests.get(url, auth=HTTPBasicAuth(unity.refresh_token, unity.secretkey), params=params)
    data = response.json()
    if response.status_code == 200:
        unity_data = data["results"]
        while data["offset"] + 1000 < data["total"]:
            offset +=1000
            params["offset"] = offset
            response = requests.get(url, auth=HTTPBasicAuth(unity.refresh_token, unity.secretkey), params=params)
            data = response.json()
            unity_data.extend(data["results"])
        return pd.DataFrame(unity_data) 
    else:
        print(f"Failed to get bundle id from Unity with response: {response.text}")    
        return pd.DataFrame()



def etl_unity_rev(today,yesterday):
    start_time = datetime.now()
    start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    end = today + timedelta(days=1)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    url = unity.url_rev

    # Add params to requests
    params = unity.params
    params["fields"] = "platform,revenue,started"
    params["splitBy"] = "source,country"
    params["scale"] = "day"
    params["start"] = start
    params["end"] = end
    
    # Make the GET request
    response = requests.get(url , params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.text
        # Transform
        df = pd.read_csv(StringIO(data), keep_default_na=False)
        bundle_map_df = get_unity_bundleid()
        df = df.merge(bundle_map_df, left_on="Source game id", right_on="gameId", how="inner")
        print(f'Success REQUEST REVENUE Unity in: {datetime.now() - start_time}')

        request_time = datetime.now()
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}
        dict_network = {network.name: network for network in network_set}

        df["Country code"] = df["Country code"].replace("", "OTH").apply(lambda x: x if x in dict_country_id else 'OTH')
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        df = df[df["storeId"].isin(dict_game_id)]
        df["revenue"] = df["revenue"].astype("float")
        df["started"] = df["started"].astype("int")
        df["revenue"] = df["revenue"] * 0.98
        df = df.groupby(['Date','Platform','Country code','storeId'])[['revenue','started']].sum().reset_index()
        df['ecpm'] = np.where(df['started'] == 0, 0, (df['revenue'] / df['started']) * 1000)
        df['ecpm'] = df["ecpm"].astype("float")
        name_dict = {
        'Date' : 'update_date', 
        'Platform' : 'platform', 
        'storeId' : 'product_id',
        'revenue' : 'revenue',
        'Country code' : 'country',
        'started' : 'impressions'
        }
        df.rename(columns=name_dict, inplace=True)
        df["network"] = 'Unity'
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/unity.csv', index=False)

        print(f'Success TRANSFORM REVENUE Unity in: {datetime.now() - request_time}')
        
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)


def etl_unity_rev_nocountry(today,yesterday):
    start_time = datetime.now()
    start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    end = today + timedelta(days=1)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    url = unity.url_rev

    # Add params to requests
    params = unity.params
    params["fields"] = "platform,revenue,started"
    params["splitBy"] = "source"
    params["scale"] = "day"
    params["start"] = start
    params["end"] = end
    
    # Make the GET request
    response = requests.get(url , params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.text
        # Transform
        df = pd.read_csv(StringIO(data), keep_default_na=False)
        bundle_map_df = get_unity_bundleid()
        df = df.merge(bundle_map_df, left_on="Source game id", right_on="gameId", how="inner")
        print(f'Success REQUEST REVENUE Unity in: {datetime.now() - start_time}')

        request_time = datetime.now()
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_network = {network.name: network for network in network_set}

        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        df = df[df["storeId"].isin(dict_game_id)]
        df["revenue"] = df["revenue"].astype("float")
        df["started"] = df["started"].astype("int")
        df["revenue"] = df["revenue"] * 0.98
        df = df.groupby(['Date','Platform','storeId'])[['revenue','started']].sum().reset_index()
        df['ecpm'] = np.where(df['started'] == 0, 0, (df['revenue'] / df['started']) * 1000)
        df['ecpm'] = df["ecpm"].astype("float")
        name_dict = {
            'Date' : 'update_date', 
            'Platform' : 'platform', 
            'storeId' : 'product_id',
            'revenue' : 'revenue',
            'started' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["network"] = 'Unity'
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/unity.csv', index=False)

        print(f'Success TRANSFORM REVENUE NOCOUNTRY Unity in: {datetime.now() - request_time}')

    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)

def etl_unity_cost(today,yesterday):
    def strip_whitespace(value):
        return value.strip() if isinstance(value, str) else value
    now = datetime.now()

    start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")
    end = today + timedelta(days=1)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")
    
    url = unity.url_cost

    # Add params to requests
    params = unity.params
    params["fields"] = "timestamp,country,campaignSet,platform,country,installs,spend"
    params["splitBy"] = "platform,country,campaignSet"
    params["scale"] = "day"
    params["start"] = start
    params["end"] = end
    # Make the GET request
    response = requests.get(url , params=params)

    print(f'Request Cost Data from Unity Success in {datetime.now() - now}')
    now = datetime.now()

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.text

        # Transform
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_network = {network.name: network for network in network_set}
        game_df = pd.DataFrame(game_set.values())
        country_set = Country.objects.all()
        dict_country_id = {country.country_id: country  for country in country_set}

        df = pd.read_csv(StringIO(data), keep_default_na=False)
        df = df.apply(lambda x: x.map(strip_whitespace) if x.dtype == 'object' else x)
        df.columns = [col.replace("ï»¿", "") for col in df.columns]

        bundle_map_df = unity_bundleid_cost()
        if len(bundle_map_df) > 0:
            df = df.merge(bundle_map_df, left_on="campaign set id", right_on="id", how="inner")
            df["Date"] = pd.to_datetime(df["timestamp"]).dt.date
            df = df[df["storeId"].isin(dict_game_id)]
            df['platform'] = df['platform'].str.lower()
            df['country'] = df['country'].str.upper().apply(lambda x: x if x in dict_country_id else 'OTH')
            df['network'] = 'Unity'

            if df['Date'].nunique() == 2:
                df = df.groupby(['Date','country','storeId','network'])[['spend','installs']].sum().reset_index()
                name_dict = {
                    'Date' : 'update_date', 
                    'country': 'country', 
                    'storeId' : 'product_id',
                    'spend' : 'cost',
                    'installs' : 'installs'
                    }
                df.rename(columns=name_dict, inplace=True)

                # Fill nan
                fill_values = {'installs': 0, 'cost': 0}
                df.fillna(value=fill_values, inplace=True)

                print(f'Transform Cost Data from Unity with {len(df)} rows  Success in {datetime.now() - now}')
                return df
            else:
                print(f"Cost data from Unity on {end} is not ready")
                return pd.DataFrame()
        else:
            print(f"Can not get Bundle_id from Unity API")
            return pd.DataFrame()
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        return pd.DataFrame()

def run_unity():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_unity_rev(today,yesterday)
    etl_unity_rev_nocountry(today,yesterday)


