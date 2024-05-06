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
from datetime import datetime, timedelta
from network_config import pangle
import pandas as pd
import numpy as np
import logging
import json
from sign_generate import PangleMediaUtil
import hashlib

def get_ios_bundle():
    NONCE = 20
    security_key = pangle.secure_key
    timestamp = int(datetime.now().timestamp())
    nonce = NONCE
    keys = [security_key, str(timestamp), str(nonce)] 
    keys.sort() 
    keyStr = ''.join(keys) 
    signature = hashlib.sha1(keyStr.encode("utf-8")).hexdigest() 
    url = 'https://open-api.pangleglobal.com/union/media/open_api/site/query'  # Replace with the actual API URL

    headers = {
        'content-type': 'application/json',
        'accept': 'application/json',
        'X-Tt-env': 'open_api_sandbox'
    }

    data_post = {
        'user_id': int(pangle.role_id),
        'role_id': int(pangle.role_id),
        'timestamp': timestamp,
        'nonce': NONCE,
        'sign': signature,
        'version': "1.0",
        'page': 1,
        'page_size': 500
    }

    response = requests.post(url, json=data_post, headers=headers)
    data = response.json()["data"]
    page = 1
    app_data = [] 
    while page <= data["page_info"]["total_page"]:
        data_post["page"] = page
        response = requests.post(url, json=data_post, headers=headers)
        data = response.json()["data"]
        app_data.extend(data["app_list"])
        page += 1
    app_df = pd.DataFrame(app_data)
    app_df["ios_bundle"] = app_df['download_url'].str.extract(r'/id(\d+)')
    
    return app_df[["app_id", "ios_bundle"]]

def get_tiktok_ad(advertiser_id):
    url_ad = "https://business-api.tiktok.com/open_api/v1.3/ad/get/"
    url_app = "https://business-api.tiktok.com/open_api/v1.3/app/list/"
    headers = pangle.headers
    params = {
        "advertiser_id": advertiser_id
    }
    # Get app
    response = requests.get(url_app, params=params, headers=headers)
    if  len(response.json()["data"]["apps"]) > 0:
        app_df = pd.DataFrame(response.json()["data"]["apps"])

        # get ad
        page = 1 
        params["page"] = page
        params["page_size"] = 1000
        
        response = requests.get(url_ad, params=params, headers=headers)
        if response.json()["code"] == 0:
            data = response.json()["data"]
            
            ad_data=[]
            while page <= data["page_info"]["total_page"]:
                params["page"] = page
                response = requests.get(url_ad, params=params, headers=headers)
                if response.json()["code"] == 0:
                    page += 1
                    data = response.json()["data"]
                    ad_data.extend(data["list"])
                else: 
                    print(response.text)
                    break

            ad_df = pd.DataFrame(ad_data)
            ad_df = ad_df[["ad_id","app_name"]].merge(app_df[["app_name", "package_name", "platform"]], on="app_name", how="inner")
        else:
            print(f"{advertiser_id} get error: {response.text}")
            return pd.DataFrame()
    else:
        print(f"{advertiser_id} have no ad data")
        return pd.DataFrame()

    return ad_df

def etl_pangle_rev(today,yesterday):
    start_time =datetime.now()
    pangle_data = []

    country_set = Country.objects.all()
    game_set = Game.objects.all()
    network_set = Network.objects.all()
    dict_game_id = {game.id_bundle: game for game in game_set}
    dict_country_id = {country.country_id: country for country in country_set}
    dict_country_id.update({"OTHER": "OTH"})
    dict_network = {network.name: network for network in network_set}

    date_list = [yesterday + timedelta(days=i) for i in range((today - yesterday).days + 1)]
    for date in date_list:
        
        date = date.strftime("%Y-%m-%d")
        params = pangle.params
        url = pangle.url_rev
            
        for country in dict_country_id:
            try:
                params["date"] = date
                # params["dimensions"] = "os"
                params["region"] = country.lower()

                params_condition = {
                    "time_zone": params["time_zone"],
                    "currency": params["currency"],
                    "date": params["date"],
                    "region": params["region"]
                }
                sign = PangleMediaUtil.get_sig(params_condition)
                params["sign"] = sign

                # Make the GET request
                response = requests.get(url ,params=params)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse the JSON response
                    data = response.json()
                    if data['Code'] != '100':
                        print(f"{country} Failed")
                        continue
                    result_data = data["Data"][date]
                    
                    # Transform
                    for row in result_data:
                        row["os"] = row["os"].lower().strip()
                        row["package_name"] = row["package_name"].strip()
                        row["region"] = row["region"].upper().strip()
                        if row["region"] == 'OTHER':
                            row["region"] = 'OTH'
                    filtered_data = [row for row in result_data if int(row["show"]) != 0]

                    pangle_data.extend(filtered_data)
                else:
                    print(f"Request failed with status code: {response.status_code}")
                    print(response.text)
            except Exception as e:
                logging.error(e)

    print(f"Request Pangle Success in: {datetime.now() - start_time}")
    request_time = datetime.now()

    df = pd.DataFrame(pangle_data)
    app_df = get_ios_bundle()
    df = df.merge(app_df, on ='app_id', how='left')
    df['package_name'] = np.where((df['os'] == 'ios') & (df['ios_bundle'].notna()), df['ios_bundle'], df['package_name'])
    df = df[df['package_name'].isin(dict_game_id)]
    df["revenue"] = df["revenue"].astype("float")
    df["show"] = df["show"].astype("int")

    df = df.groupby(['date','os','region','package_name'])[['revenue','show']].sum().reset_index()
    name_dict = {
        'date' : 'update_date', 
        'os' : 'platform',
        'region': 'country',  
        'package_name' : 'product_id',
        'revenue' : 'revenue',
        'show' : 'impressions'
        }
    df.rename(columns=name_dict, inplace=True)
    df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
    df["network"] = 'Pangle'
    
    fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
    df.fillna(value=fill_values, inplace=True)
    df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

    df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/pangle.csv',index = False)

    # No country
    df = df.groupby(['update_date','platform','product_id'])[['revenue','impressions']].sum().reset_index()
    df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
    df["network"] = 'Pangle'

    df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/pangle.csv',index = False)


    print(f"Transform Pangle Success in: {datetime.now() - request_time}")

def etl_tiktok_cost(today,yesterday):
    def concat_name_platform(row):
        result = row["name"] +" (" + row["platform"]  +")" 
        return result

    now = datetime.now()
    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    url = pangle.url_cost
    headers = pangle.headers

    params = pangle.data
    params["service_type"] = "AUCTION"
    params["report_type"] = "BASIC"
    params["data_level"] = "AUCTION_AD"
    params["dimensions"] = '["country_code","ad_id","stat_time_day"]'
    params["metrics"] = '["spend","app_install"]'
    params["start_date"] = start
    params["end_date"] = end
    params["order_field"] = "spend"
    params["order_type"] = "DESC"
    params["page_size"] = 1000

    game_set = Game.objects.all()
    network_set = Network.objects.all()
    dict_game_id = {game.id_bundle: game for game in game_set}
    dict_network = {network.name: network for network in network_set}
    country_set = Country.objects.all()
    dict_country_id = {country.country_id: country  for country in country_set}

    advertiser_id_path ="/home/mkt-en/mkt_dashboard/conf/tiktok.csv"
    df_advertiser_id = pd.read_csv(advertiser_id_path)
    result_df = pd.DataFrame()
    
    for index, row in df_advertiser_id.iterrows():
        advertiser_id = row["advertiser_id"]
        params["advertiser_id"] = advertiser_id
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        page = 1
        result_data = []
        while page <= data["data"]["page_info"]["total_page"]:
            params["page"] = page
            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            page += 1
            data_list = data["data"]["list"]
            for item in data_list:
                metrics = item["metrics"]
                dimensions = item["dimensions"] 
                row = {
                    "stat_time_day": dimensions["stat_time_day"],
                    "ad_id": dimensions["ad_id"],
                    "country": dimensions["country_code"],
                    "spend": metrics["spend"],
                    "app_install": metrics["app_install"]
                }
                result_data.append(row)

        print(f"Request for Adtiviser_id {advertiser_id} success in: {datetime.now() - now}")
        now = datetime.now()

        if len(result_data) > 0:         
            ad_df = get_tiktok_ad(advertiser_id)
            if len(ad_df) > 0:
                df = pd.DataFrame(result_data)
                df = df.merge(ad_df, on="ad_id", how="inner")
                
                df['stat_time_day'] = pd.to_datetime(df['stat_time_day']).dt.date
                df["platform"] = df["platform"].str.lower().str.strip()
                df["package_name"] = df["package_name"].str.strip()
                df['country'] = df['country'].str.upper().str.strip().apply(lambda x: x if x in dict_country_id else 'OTH')
                df = df[df["package_name"].isin(dict_game_id)]
                df = df[['stat_time_day','country','platform', 'package_name','spend', 'app_install']]
                result_df = pd.concat([result_df, df], ignore_index=True)
        
    result_df['spend'] = result_df['spend'].astype('float')
    result_df['app_install'] = result_df['app_install'].astype('int')
    result_df['network'] = 'Tiktok'

    if result_df["stat_time_day"].nunique() ==2:
        result_df = result_df.groupby(['stat_time_day','country','package_name','network'])[['spend','app_install']].sum().reset_index()    
        name_dict = {
            'stat_time_day' : 'update_date', 
            'country': 'country', 
            'package_name' : 'product_id',
            'spend' : 'cost',
            'app_install' : 'installs'
            }
        result_df.rename(columns=name_dict, inplace=True)

        # Fill nan
        fill_values = {'installs': 0, 'cost': 0}
        result_df.fillna(value=fill_values, inplace=True)

        print(f"Transform Tiktok cost with {len(result_df)} rows success in: {datetime.now() - now}")
        
        return result_df
    
    else:
        print(f"Cost Data from Tiktok of {end} is not ready")
        return pd.DataFrame()

def run_pangle():
    today = datetime.now()
    yesterday = today - timedelta(days=2)
    etl_pangle_rev(today,yesterday)



    

    




    


