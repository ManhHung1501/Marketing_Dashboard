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
from requests_oauthlib import OAuth1
from network_config import fyber
import json


def get_access_token():
    url = 'https://console.fyber.com/api/v2/management/auth'
    body = {
        
      "grant_type": "management_client_credentials",
      "client_id": fyber.client_id,
      "client_secret": fyber.client_secret
        
    }
    response = requests.post(url, data =body)
    
    return response.json()["accessToken"]

def get_bundle_id(appId):
    file_path = '/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/fyber_bundle.json'
    if os.path.exists(file_path):
        with open(file_path, 'r') as json_file:
            # Load JSON data from the file
            data = json.load(json_file)
    else:
        data={}
        
     
    if data.get(appId)==None:
        url = 'https://console.fyber.com/api/management/v1/app?='
        accesstoken = get_access_token()
        headers = {
            'Authorization' : f"Bearer {accesstoken}"
        }
        params = {
            "appId": f"{appId}"
        }
        response_data = {}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                response_data = response.json()
                if 'bundle' in response_data:
                    with open(file_path, 'w') as json_file:
                        data[appId] = response_data
                        json.dump(data, json_file, indent=4)
            else:
                return None
        except Exception as e:
            print(f'Request bundle_id of {appId} get error {e}')
            return None
       
    return data[appId]['bundle']

def etl_fyber_rev(today,yesterday):
    start_time = datetime.now()

    start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    start = int(start.timestamp())
    end = int(today.timestamp())
    oauth = OAuth1(
        fyber.oauth_consumer_key,
        client_secret=fyber.oauth_signature,
    ) 
    url = fyber.url_rev.format(fyber.publisherId, start, end)
        
    # Make the GET request
    try:
        response = requests.get(url, auth=oauth , timeout=120)
    except Exception as e:
        return 0

    print(f'Success REQUEST REVENUE Fyber in: {datetime.now() - start_time}')
    request_time = datetime.now()

    if response.status_code == 200:
        # Parse the JSON response
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}
       
        data = response.json()['apps']
        trans_data = []
        for app in data:
            app_bundle_id = get_bundle_id(app['appId'])
            if app_bundle_id == None:
                print(f'{app["appId"]} not have bundle id')
                continue
            for spot in app["spots"]:
                for item in spot['units']:
                    # transform timestamp to date
                    item['date'] = datetime.fromtimestamp(item['date'])
                    item['date'] = item['date'].strftime("%Y-%m-%d")
                    
                    #transform country
                    item["country"] = item["country"].upper().strip()
                    if item["country"] not in dict_country_id:
                        item["country"] = 'OTH'
                    item['package_name'] = app_bundle_id
                    
                    #transform platform
                    if item['distributorName'] == 'iPhone':
                        item['distributorName'] = 'ios'
                    else:  
                        item['distributorName'] = 'android'
                    #assign package name
                    if item['package_name'] in dict_game_id:
                        trans_data.append(item)
        df = pd.DataFrame(trans_data)
    
        df['revenue'] = df['revenue'].astype('float')
        df['impressions'] = df['impressions'].astype('int')
        df = df.groupby(['date','distributorName','country','package_name'])[['revenue','impressions']].sum().reset_index()
        name_dict = {
            'date' : 'update_date', 
            'distributorName' : 'platform', 
            'country': 'country', 
            'package_name' : 'product_id',
            'revenue' : 'revenue',
            'impressions' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
        df["network"] = 'Fyber'
        
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/fyber.csv', index=False)
        
        print(f'Success TRANSFORM REVENUE Fyber in: {datetime.now() - request_time}')    

    else:
        print(f"Request failed with status code: {response.status_code}: {response.text}")
        print(response.text)


def etl_fyber_rev_nocountry(today,yesterday):
    start_time = datetime.now()

    start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)   
    start = int(start.timestamp())
    end = int(today.timestamp())

    oauth = OAuth1(
        fyber.oauth_consumer_key,
        client_secret=fyber.oauth_signature,
    ) 
    url = fyber.url_rev.format(fyber.publisherId, start, end)
        
    # Make the GET request
    try:
        response = requests.get(url, auth=oauth , timeout=120)
    except Exception as e:
        return 0

    print(f'Success REQUEST REVENUE NOCOUNTRY Fyber in: {datetime.now() - start_time}')
    request_time = datetime.now()
    
    if response.status_code == 200:
        # Parse the JSON response
        game_set = Game.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
   
        data = response.json()['apps']
        trans_data = []
        for app in data:
            app_bundle_id = get_bundle_id(app['appId'])
            if app_bundle_id == None:
                continue
            for spot in app["spots"]:
                for item in spot['units']:
                    # transform timestamp to date
                    item['date'] = datetime.fromtimestamp(item['date'])
                    item['date'] = item['date'].strftime("%Y-%m-%d")
                    #transform country
                    
                    item['package_name'] = app_bundle_id
                    #transform platform
                    if item['distributorName'] == 'iPhone':
                        item['distributorName'] = 'ios'
                    else:  
                        item['distributorName'] = 'android'
                    #assign package name
                    if item['package_name'] in dict_game_id:
                        trans_data.append(item)

        df = pd.DataFrame(trans_data)
        df['revenue'] = df['revenue'].astype('float')
        df['impressions'] = df['impressions'].astype('int')
        df = df.groupby(['date','distributorName','package_name'])[['revenue','impressions']].sum().reset_index()
        name_dict = {
            'date' : 'update_date', 
            'distributorName' : 'platform', 
            'package_name' : 'product_id',
            'revenue' : 'revenue',
            'impressions' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
        df["network"] = 'Fyber'
        
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/fyber.csv', index=False)
    
        print(f'Success TRANSFORM REVENUE NOCOUNTRY Fyber in: {datetime.now() - request_time}') 

        
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
   

def run_fyber():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_fyber_rev(today, yesterday)
    etl_fyber_rev_nocountry(today, yesterday)

