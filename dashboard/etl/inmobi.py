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
from datetime import datetime,timedelta
from network_config import inmobi
from sign_generate import inmobi_session
import logging
import json


def etl_inmobi_rev(today,yesterday):
    start_time = datetime.now()
    
    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    params = inmobi.params
    headers = inmobi.headers
    try:
        session_id, account_id = inmobi_session(inmobi.userName, inmobi.secretKey)
    except:
        print("Failed to generate sessionID and accountID")
        return 0

    headers["accountId"] = account_id
    headers["sessionId"] = session_id
    
    url = inmobi.url_rev
        # Add params to requests
    params["reportRequest"]["groupBy"] = ["date","platform","inmobiAppId","country"]
    params["reportRequest"]["metrics"] = ["adImpressions", "earnings", "costPerMille"]
    params["reportRequest"]["timeFrame"] = f"{start}:{end}"
    params["reportRequest"]["filterBy"] = [{ "filterName":"adImpressions", "filterValue": "0" , "comparator":">"}]
    params["reportRequest"]["orderBy"] = ["date","platform","inmobiAppId","country"]
    params["reportRequest"]["orderType"] = "desc"
    post_data = json.dumps(params)
    response = requests.post(url, headers=headers, data=post_data)

    
    request_time = datetime.now()

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        print(f'Success REQUEST REVENUE InMobi in: {datetime.now() - start_time}')
        
        # Parse the JSON response
        data = response.json()
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_network = {network.name: network for network in network_set}
        dict_game_id = {game.id_bundle: game for game in game_set}
        game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "bundleId"})
        dict_country = {country.country_name: country.country_id for country in country_set}
        dict_country_id = {country.country_id: country for country in country_set}
        dict_country.update({"Turks And Caicos Islands": "TC", "Lao People'S Democratic Republic":"LA",'Republic Of Moldova':"MD", 'Occupied Palestinian Territory':"PS", 'Bosnia And Herzegovina': 'BA', 'Vietnam':'VN', 'Colombia':'CO', 'Brunei Darussalam':'BN', 'Micronesia':'FM', 'Macedonia':'MK', 'Trinidad And Tobago':'TT', 'Tanzania, United Republic Of':'TZ', 'Saint Vincent And The Grenadines':'VC', 'Czech Republic':'CZ', 'Gambia':'GM', 'Bahamas':'BS', 'Saint Kitts And Nevis':'KN', 'Curacao':'CW', 'Virgin Islands, British':'VG', 'Congo':'CD', 'East Timor':'TL', 'Wallis And Futuna':'WF', 'Reunion':'RE', 'Antigua And Barbuda':'AG', 'Congo, The Democratic Republic Of The':'CD', "Cote D'Ivoire":'CI', 'Sao Tome And Principe':'ST', 'Swaziland':'SZ', 'Isle Of Man':'IM', 'Saint Pierre And Miquelon':'PM', 'Vatican City State': 'IT', 'Saint Helena': 'SH', 'Burma (Myanmar)': 'MM', 'The State of Palestine': 'PS'})
        offset = 1
        result_data = []
        
        while True:
            params["reportRequest"]["offset"] = offset
            params["reportRequest"]["length"] = 5000
            post_data = json.dumps(params)
            response = requests.post(url, headers=headers, data=post_data)
            if response.status_code != 200:
                continue
            data = response.json()
            if data.get("respList") is None:
                break
            offset += 5000
            
            # Transform
            for row in data["respList"]:
                row["platform"] = row["platform"].lower().strip()
                row["bundleId"] = row["bundleId"].strip()                
                row["date"] = datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S").date()
                row["country"] = dict_country.get(row["country"].strip(), 'OTH')
                
            result_data.extend(data["respList"])

        df = pd.DataFrame(result_data)
        
        df = df.merge(game_df, on =['platform', 'bundleId'], how='left')
        df['bundleId'] = np.where((df['platform'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['bundleId'])
        df = df[((df['platform'] == 'ios') & (df['id_bundle'].notna()) | (df["platform"] == 'android'))]
        df = df[df["bundleId"].isin(dict_game_id)]
        df['earnings'] = df['earnings'].astype('float')
        df['adImpressions'] = df['adImpressions'].astype('int')
        df = df.groupby(['date','platform','country','bundleId'])[['earnings','adImpressions']].sum().reset_index()
        df = df[df["adImpressions"] != 0]
        name_dict = {
            'date' : 'update_date', 
            'platform' : 'platform', 
            'country': 'country', 
            'bundleId' : 'product_id',
            'earnings' : 'revenue',
            'adImpressions' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
        df["network"] = 'InMobi'
        
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/inmobi.csv', index=False)
        
        print(f'Success TRANSFORM REVENUE InMobi in: {datetime.now() - request_time}') 
    
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
    
def etl_inmobi_rev_nocountry(today,yesterday):
    start_time = datetime.now()

    start = yesterday.strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    
    params = inmobi.params
    headers = inmobi.headers
    
    try:
        session_id, account_id = inmobi_session(inmobi.userName, inmobi.secretKey)
    except:
        print("Failed to generate sessionID and accountID")
        return 0

    headers["accountId"] = account_id
    headers["sessionId"] = session_id
    
    url = inmobi.url_rev
        # Add params to requests
    params["reportRequest"]["groupBy"] = ["date","platform","inmobiAppId"]
    params["reportRequest"]["metrics"] = ["adImpressions", "earnings", "costPerMille"]
    params["reportRequest"]["timeFrame"] = f"{start}:{end}"
    params["reportRequest"]["filterBy"] = [{ "filterName":"adImpressions", "filterValue": "0" , "comparator":">"}]
    params["reportRequest"]["orderBy"] = ["date","platform","inmobiAppId"]
    params["reportRequest"]["orderType"] = "desc"
    post_data = json.dumps(params)
    response = requests.post(url, headers=headers, data=post_data)
    
    
    request_time = datetime.now()

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        print(f'Success REQUEST REVENUE InMobi NoCountry in: {datetime.now() - start_time}')
        # Parse the JSON response
        data = response.json()
        
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        network_set = Network.objects.all()
        dict_network = {network.name: network for network in network_set}
        dict_game_id = {game.id_bundle: game for game in game_set}
        game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "bundleId"})

        offset = 1
        result_data = []
        
        while True:
            params["reportRequest"]["offset"] = offset
            params["reportRequest"]["length"] = 5000
            post_data = json.dumps(params)
            response = requests.post(url, headers=headers, data=post_data)
            
            if response.status_code != 200:
                continue
            data = response.json()    
            if data.get("respList") is None:
                break
            offset += 5000
            
            
            # Transform
            for row in data["respList"]:
                row["platform"] = row["platform"].lower().strip()
                row["bundleId"] = row["bundleId"].strip()                
                row["date"] = datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S").date()
                            
            result_data.extend(data["respList"])

        df = pd.DataFrame(result_data)
        df = df.merge(game_df, on =['platform', 'bundleId'], how='left')
        df['bundleId'] = np.where((df['platform'] == 'ios') & (df['id_bundle'].notna()), df['id_bundle'], df['bundleId'])
        df = df[((df['platform'] == 'ios') & (df['id_bundle'].notna()) | (df["platform"] == 'android'))]
        df = df[df["bundleId"].isin(dict_game_id)]
        df['earnings'] = df['earnings'].astype('float')
        df['adImpressions'] = df['adImpressions'].astype('int')
        df = df.groupby(['date','platform','bundleId'])[['earnings','adImpressions']].sum().reset_index()
        df = df[df["adImpressions"] != 0]
        name_dict = {
            'date' : 'update_date', 
            'platform' : 'platform',  
            'bundleId' : 'product_id',
            'earnings' : 'revenue',
            'adImpressions' : 'impressions'
            }
        df.rename(columns=name_dict, inplace=True)
        df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
        df["network"] = 'InMobi'
        
        fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
        df.fillna(value=fill_values, inplace=True)

        df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/inmobi.csv', index=False)

        print(f'Success TRANSFORM REVENUE InMobi NoCountry in: {datetime.now() - request_time}')
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
 

def run_inmobi():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_inmobi_rev(today,yesterday)
    etl_inmobi_rev_nocountry(today,yesterday)



