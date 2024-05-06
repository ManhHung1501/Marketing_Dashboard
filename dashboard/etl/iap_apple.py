import sys
sys.path.insert(0, '/home/mkt-en/mkt_dashboard')
import os
import django
# Configure Django settings
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mkt_dashboard.settings")
django.setup()
from django.db import connection
from mkt_dashboard.settings import DATABASES
from dashboard.models import Game,Country,Exchange
import jwt
import time
import pandas as pd
import numpy as np
import requests
import gzip
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from datetime import time as t
import json
import logging


def generate_token():
    tree = ET.parse('/home/mkt-en/mkt_dashboard/conf/appstore.xml')
    root = tree.getroot()
    key_id = root.find("key_id").text
    issuer_id = root.find("issuer_id").text
    vendor_number = root.find("vendor_number").text
    
    with open('/home/mkt-en/mkt_dashboard/conf/apple/AuthKey_6WF96W4ST7.p8', 'r') as myfile:
        private_key = myfile.read()

    iat = time.time()
    exp = iat + 20 * 60

    payload = {
        "iss": issuer_id,
        "exp": exp,
        "aud": "appstoreconnect-v1",
    }
    headers = {
        "alg": "ES256",
        "kid": key_id,
        "typ": "JWT"
    }

    token = jwt.encode(payload, private_key, headers=headers, algorithm='ES256')

    return token

def exchange_rate(DATE, API_KEY = "80a4f6153bfcf1f6dc930130"):
    try:
        response = Exchange.objects.get(date_update = DATE).rate
    except Exchange.DoesNotExist:
        request_url = "https://v6.exchangerate-api.com/v6/{}/latest/USD/".format(API_KEY)
        res_url = requests.get(request_url)
        if res_url.status_code != 200:
            API_KEY = "653e3dd70c8ccce6b05fbe03"
            request_url = "https://v6.exchangerate-api.com/v6/{}/latest/USD/".format(API_KEY)
            print(request_url)
            res_url = requests.get(request_url)
            if res_url.status_code != 200:
                if res_url.status_code == 404:
                    print('There is a problem with the request URL. Make sure that it is correct')
                else:
                    print('There was a problem retrieving data: ', res_url.text, res_url.status_code)
                return res_url.text
        response = res_url.json()["conversion_rates"]
        data = Exchange(date_update = DATE, rate = response)
        data.save()
    return response

def etl_iap_apple(start, end):
    def strip_whitespace(value):
        return value.strip() if isinstance(value, str) else value

    tree = ET.parse('/home/mkt-en/mkt_dashboard/conf/appstore.xml')
    root = tree.getroot()
    vendor_number = root.find("vendor_number").text
    
    try:
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}
        exchange_df = pd.DataFrame(Exchange.objects.all().values())
        exchange_df["date_update"] = exchange_df["date_update"].astype(str)
        game_df = pd.DataFrame(game_set.values()).rename(columns={"id_track": "Apple Identifier"})

        result_df = pd.DataFrame()
        list_date = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        for date_update in list_date:
            
            yesterday = datetime.now() - timedelta(days=1)
            if date_update.time() < t(15, 0, 0) and date_update.date() == yesterday.date():
                print(f'Data of {date_update} not ready')
                break
            
            start_time = datetime.now()

            date_update = date_update.strftime('%Y-%m-%d')        
            exchange_rate(date_update)
        
            # Define the API endpoint URL
            url = "https://api.appstoreconnect.apple.com/v1/salesReports"

            # Define the query parameters (filters)
            params = {
                "filter[frequency]": "DAILY",
                "filter[reportDate]": date_update,
                "filter[reportSubType]": "SUMMARY",
                "filter[reportType]": "SALES",
                "filter[vendorNumber]": str(vendor_number)
            }

            # Add authentication headers here 
            headers = {
                "Authorization": f"Bearer {generate_token()}"
            }

            response = requests.get(url, params=params, headers=headers)
            compressed_content = response.content

            print(f"Reuqest APPLE IAP of {date_update} Success in: {datetime.now() - start_time}")
            request_time = datetime.now()

            # Decompress the content using gzip
            with gzip.GzipFile(fileobj=io.BytesIO(compressed_content), mode='rb') as uncompressed_file:
                # Read the uncompressed content
                uncompressed_data = uncompressed_file.read()
            # Decode the bytes to a string
            data_string = uncompressed_data.decode('utf-8')

            # Create a DataFrame from the data using pd.read_csv with a custom separator
            df = pd.read_csv(io.StringIO(data_string), sep="\t", keep_default_na=False)
            df = df.apply(lambda x: x.map(strip_whitespace) if x.dtype == 'object' else x)
            df.replace("",np.nan, inplace=True)

            df['date_update'] = pd.to_datetime(df['Begin Date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
            df["Country Code"]  = df["Country Code"].str.upper().apply(lambda x: x if x in dict_country_id else 'OTH')
            df["Apple Identifier"] = np.where(df["Parent Identifier"].notna(), df["Parent Identifier"], df["Apple Identifier"])
            df["Apple Identifier"] = df["Apple Identifier"].astype(str)
            df['platform'] ='ios'
            df = df.merge(game_df, on=["Apple Identifier","platform"], how="left")
            df["Apple Identifier"] = np.where((df['id_bundle'].notna()) & (df['platform'] =='ios'), df['id_bundle'], df['Apple Identifier'])
            df = df[df["Apple Identifier"].isin(dict_game_id)]
            df["Units"] = df["Units"].astype("int")
            df["Developer Proceeds"] = df["Developer Proceeds"].astype("float")
            df["revenue"] = df["Units"] * df["Developer Proceeds"]
            df = df.merge(exchange_df, on='date_update', how='left')
            df['rate'] = df.apply(lambda row: row['rate'].get(row['Customer Currency'], 1), axis=1)

            df = df.groupby(['date_update','Country Code','platform','Apple Identifier','rate'])['revenue'].sum().reset_index()
            df['rate'] = df['rate'].astype("float")
            df["revenue"] = 0.9 * df["revenue"]/df['rate']
            df =df[df["revenue"] != 0]

            name_dict = {
            'date_update' : 'update_date', 
            'platform' : 'platform', 
            'Country Code': 'country',
            'Apple Identifier' : 'product_id',
            'revenue' : 'revenue'
            }
            df.rename(columns = name_dict, inplace  = True)
            df= df[["update_date", "platform", "country", "product_id", "revenue"]]

            result_df = pd.concat([result_df, df], ignore_index=True)
            
            print(f"Transform APPLE IAP of {date_update} Success in: {datetime.now() - start_time}")

        result_df= result_df[["update_date", "platform", "country", "product_id", "revenue"]]
    
        result_df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/iap/apple.csv', index=False)
    
    except Exception as e:
        print(f"get IAP from apple get error: {e}")


def run_apple_iap():
    today = datetime.now()
    start = today - timedelta(days=3)
    end = today - timedelta(days=1)
    etl_iap_apple(start, end)
