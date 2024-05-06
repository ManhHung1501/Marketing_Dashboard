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
from network_config import mintegral
from sign_generate import sign_mintegral
import logging
import requests
import time
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime, timedelta
import math

def get_campaign():
    url ='https://ss-api.mintegral.com/api/open/v1/campaign'
    headers = mintegral.headers
    time_req, token = sign_mintegral(mintegral.apikey_cost)

    headers["access-key"] = mintegral.access_key
    headers["token"] = token
    headers["timestamp"] = time_req

    params= {
        'limit': 50,
        'page': 1
    }

    campaign_data = []
    response = requests.get(url, params=params, headers=headers)
    data = response.json()["data"]
    total_page =math.ceil(data['total']/50)
    page = 1
    if response.status_code == 200:
        while page <=total_page:
            params["page"] = page
            response = requests.get(url, params=params, headers=headers)
            data = response.json()["data"]
            campaign_data.extend(data["list"])
            page += 1

        df = pd.DataFrame(campaign_data)
    else:
        print(f"get campaign from Mintegral Error: {response.text}")
    
    return df[["campaign_id", "package_name", "platform"]]

def etl_mintegral_rev(today,yesterday):
    start_time = datetime.now()
    start = yesterday.strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    url = mintegral.url_rev

    # Add params to requests
    params = mintegral.params
    params["group_by"] = "date,country,app_id,platform"
    params["start"] = start
    params["end"] = end
    
    # generate sign params
    time_req, sign = sign_mintegral(mintegral.secret_key)
    params["time"] = time_req
    params["sign"] = sign
    # Make the GET request
    response = requests.get(url , params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()["data"]
        result_data =[]
        page = 1

        country_set = Country.objects.all()
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}
        dict_network = {network.name: network for network in network_set}
        while page  <= data["total_page"]:
            time_req, sign = sign_mintegral(mintegral.secret_key)
            params["time"] = time_req
            params["sign"] = sign
            params["page"] = page
            response = requests.get(url , params=params)

            print(f'Success REQUEST REVENUE Mintegral page {page} in: {datetime.now() - start_time}')
            start_time = datetime.now()

            page += 1
            data = response.json()["data"]

            # Transform
            for row in data["lists"]:
                row["platform"] = row["platform"].lower().strip()
                row["app_package"] = row["app_package"].strip() 
                row["date"] = datetime.strptime(str(row["date"]), "%Y%m%d").date()
                row["country"] = row["country"].upper().strip()
                if row["country"] not in dict_country_id:
                    if row["country"] == 'UK':
                        row["country"] ='GB'
                    else: 
                        row["country"] = 'OTH'
                if row["app_package"] not in dict_game_id:
                    if row["app_package"].startswith("id"):
                        row["app_package"] = row["app_package"].lstrip("id")
                
            
            filtered_data = [row for row in data["lists"] if row["app_package"] in dict_game_id]
            result_data.extend(filtered_data)

        request_time = datetime.now()

        df = pd.DataFrame(result_data)
        df = df.groupby(['date','platform','country','app_package'])[['est_revenue','impression']].sum().reset_index()
        name_dict = {
            'date' : 'update_date', 
            'platform' : 'platform',
            'country': 'country',  
            'app_package' : 'product_id',
            'est_revenue' : 'revenue',
            'impression' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
        df["network"] = 'Mintegral'
        
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]
        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/mintegral.csv', index=False)

        print(f'Success TRANSFORM REVENUE Mintegral in: {datetime.now() - request_time}')

    else:
        logging.info(f"Request failed with status code: {response.status_code}")
        logging.info(response.text)

def etl_mintegral_rev_nocountry(today,yesterday):
    start_time = datetime.now()
    start = yesterday.strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    
    url = mintegral.url_rev

    # Add params to requests
    params = mintegral.params
    params["group_by"] = "date,app_id,platform"
    params["start"] = start
    params["end"] = end
    
    # generate sign params
    time_req, sign = sign_mintegral(mintegral.secret_key)
    params["time"] = time_req
    params["sign"] = sign
    # Make the GET request
    response = requests.get(url , params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()["data"]
        
        result_data =[]
        page = 1

        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_network = {network.name: network for network in network_set}
        
        while page  <= data["total_page"]:
            time_req, sign = sign_mintegral(mintegral.secret_key)
            params["time"] = time_req
            params["sign"] = sign
            params["page"] = page
            response = requests.get(url , params=params)

            print(f'Success REQUEST REVENUE Mintegral NoCountry page {page} in: {datetime.now() - start_time}')
            start_time = datetime.now()

            if response.status_code != 200:
                continue
            page += 1
            data = response.json()["data"]

            # Transform
            for row in data["lists"]:
                row["platform"] = row["platform"].lower().strip()
                row["app_package"] = row["app_package"].strip() 
                row["date"] = datetime.strptime(str(row["date"]), "%Y%m%d").date()
                if row["app_package"] not in dict_game_id:
                    if row["app_package"].startswith("id"):
                        row["app_package"] = row["app_package"].lstrip("id")    
        
            filtered_data = [row for row in data["lists"] if row["app_package"] in dict_game_id]
            result_data.extend(filtered_data)    
        
        request_time = datetime.now()

        df = pd.DataFrame(result_data)
        df = df.groupby(['date','platform','app_package'])[['est_revenue','impression']].sum().reset_index()
        name_dict = {
            'date' : 'update_date', 
            'platform' : 'platform', 
            'app_package' : 'product_id',
            'est_revenue' : 'revenue',
            'impression' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
        df["network"] = 'Mintegral'
        
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)
        df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]
        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/mintegral.csv', index=False)

        print(f'Success TRANSFORM REVENUE Mintegral NoCountry in: {datetime.now() - request_time}')
    else:
        logging.info(f"Request failed with status code: {response.status_code}")
        logging.info(response.text)

def etl_mintegral_cost(today, yesterday):
    def strip_whitespace(value):
        return value.strip() if isinstance(value, str) else value

    now = datetime.now()

    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    url = mintegral.url_cost
    
    # generate token
    headers = mintegral.headers
    time_req, token = sign_mintegral(mintegral.apikey_cost)
    headers["access-key"] = mintegral.access_key
    headers["token"] = token
    headers["timestamp"] = time_req
    
    # Add params to requests
    params = mintegral.params_cost
    params["dimension_option"] = "Campaign,Location"
    params["time_granularity"] = "daily"
    params["type"] = "1"
    params["start_time"] = start
    params["end_time"] = end

    # Make the GET request
    response = requests.get(url ,headers=headers, params=params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        i = 0
        while data["code"] != 200:
            response = requests.get(url ,headers=headers, params=params)
            data = response.json()
            if data["code"] == 400:
                print(response.text)
                return pd.DataFrame()
            elif data["code"] != 200 and i < 10:
                i += 1
                time.sleep(60)
            else:
                break
        params["type"] = "2"
        response = requests.get(url ,headers=headers, params=params)
        data = response.text
       
        print(f'Request Mintegtal Cost Success in: {datetime.now() - now}')
        now = datetime.now()

        # Transform
        game_set = Game.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        country_set = Country.objects.all()
        dict_country_id = {country.country_id: country  for country in country_set}
        network_set = Network.objects.all()
        dict_network = {network.name: network for network in network_set}
    

        campaign_df = get_campaign()
        df = pd.read_csv(StringIO(data), sep="\t",keep_default_na=False)
        df = df.apply(lambda x: x.map(strip_whitespace) if x.dtype == 'object' else x)
        
        df = df.merge(campaign_df, left_on="Campaign Id", right_on='campaign_id', how="inner")
        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.date
        df["Location"] = df["Location"].str.upper().str.strip().apply(lambda x: x if x in dict_country_id else 'OTH')
        df["platform"] = df["platform"].str.lower()
        df["id_bundle"] = df["package_name"].str.strip()
        df['id_bundle'] = df['id_bundle'].str.replace(r'^id(\d+)$', r'\1', regex=True)
        df = df[df['id_bundle'].isin(dict_game_id)]
        df["Spend"] = df["Spend"].astype("float")
        df["Conversion"] = df["Conversion"].astype("int")  
        df['network'] = 'Mintegral'
        
        if df["Date"].nunique() ==2:
            df = df.groupby(['Date','Location','id_bundle','network'])[['Spend','Conversion']].sum().reset_index()
            name_dict = {
                'Date' : 'update_date', 
                'Location': 'country', 
                'id_bundle' : 'product_id',
                'Spend' : 'cost',
                'Conversion' : 'installs'
                }
            df.rename(columns=name_dict, inplace=True)

            # Fill nan
            fill_values = {'installs': 0, 'cost': 0}
            df.fillna(value=fill_values, inplace=True)

            print(f'Transform Mintegtal Cost with {len(df)} rows Success in: {datetime.now() - now}')
            return df
        else:
            print(f"Cost Data from Mintegral of {end} is not ready")
            return pd.DataFrame()
        
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        return pd.DataFrame()
 

def run_mintegral():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_mintegral_rev(today,yesterday)
    etl_mintegral_rev_nocountry(today,yesterday)

