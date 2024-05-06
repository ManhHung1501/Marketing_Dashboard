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
from googleapiclient import discovery
from googleapiclient.http import build_http
from apiclient.discovery import build
from googleapiclient.errors import HttpError
import sys
from dashboard.management.services.telegram_service import send_msg
import asyncio
from datetime import date
from datetime import timedelta
import google.oauth2.credentials
import csv
import json
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta, time as datetime_time
from google.auth.transport.requests import Request
import socket
from google.auth import exceptions
socket.setdefaulttimeout(15 * 60)
f = open ('/home/mkt-en/mkt_dashboard/conf/admob.json', "r")
data = json.loads(f.read())
f.close()


async def send_telegram_msg(msg, channel):
    await send_msg(msg, channel)

def to_plain_report(response):
    result = []

    for report_line in response:
        if report_line.get('row'):
            row = report_line.get('row')
            dm = {}
            if row.get('dimensionValues'):
                for key, value in row['dimensionValues'].items():
                    dm[key] = value['value']
                    if 'displayLabel' in value:
                        dm[key + '_NAME'] = value['displayLabel']
            if row.get('metricValues'):
                for key, value in row['metricValues'].items():
                    if 'microsValue' in value:
                        dm[key] = value['microsValue']
                    elif 'integerValue' in value:
                        dm[key] = value['integerValue']
                    elif 'doubleValue' in value:
                        dm[key] = value['doubleValue']
            result.append(dm)
    return result

def to_plain_app(response):
    result = []

    apps = response.get('apps', [])

    for app_data in apps:
        app_info = {}
        app_info['name'] = app_data.get('manualAppInfo', {}).get('displayName', '')
        app_info['appId'] = app_data.get('appId', '')
        app_info['platform'] = app_data.get('platform', '')
        app_info['appStoreId'] = app_data.get('linkedAppInfo', {}).get('appStoreId', '')
        app_info['approvalState'] = app_data.get('appApprovalState', '')

        result.append(app_info)

    return result


class AdMobAPI:

    def __init__(self):
        credentials = google.oauth2.credentials.Credentials(**data)
        credentials.expiry = datetime.strptime( 
                credentials.expiry.rstrip("Z").split(".")[0], "%Y-%m-%dT%H:%M:%S" 
                ) 
        
        # If credential is expired, refresh it.
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
            # Store JSON representation of credentials in file.
            with open("/home/mkt-en/mkt_dashboard/conf/admob.json", "w") as outfile:
                outfile.write(credentials.to_json())
                print("Success write file")
        API_SERVICE_NAME = 'admob'
        API_VERSION = 'v1'
        self.admob = build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

    def accounts(self):
        return self.admob.accounts().list().execute()

    def account(self, publisher_id):
        return self.admob.accounts().get(name=self.__accounts_path(publisher_id)).execute()

    def app_list(self, publisher_id):
        return self.admob.accounts().apps().list(parent=self.__accounts_path(publisher_id)).execute()

    def network_report(self, publisher_id, report_spec):
        request = {'reportSpec': report_spec}
        return self.admob.accounts().networkReport().generate(
            parent=self.__accounts_path(publisher_id),
            body=request).execute()

    def mediation_report(self, publisher_id, report_spec):
        request = {'reportSpec': report_spec}
        return self.admob.accounts().mediationReport().generate(
            parent=self.__accounts_path(publisher_id),
            body=request).execute()

    @staticmethod
    def __accounts_path(publisher_id):
        return f"accounts/{publisher_id}"

def etl_admob_rev(today, yesterday):
    country_set = Country.objects.all()
    game_set = Game.objects.all()
    dict_game_id = {game.id_bundle: game for game in game_set}
    dict_country_id = {country.country_id: country for country in country_set}

    now = datetime.now()

    api = AdMobAPI()

    # Get my account
    accounts = api.accounts()
    # print(accounts)
    list_date = [yesterday + timedelta(days=i) for i in range((today - yesterday).days + 1)]
    result_df = pd.DataFrame()
    try:
        for date in list_date:
        
            print(f"Begin Get Revenue Data of {date.date()} From Admob ")

            # Prepare report spec for the week
            date_range = {'startDate': {'year': date.year, 'month': date.month, 'day': date.day},
                        'endDate': {'year': date.year, 'month': date.month, 'day': date.day}}
            dimensions = ['DATE', 'APP', 'PLATFORM', 'COUNTRY']
            metrics = ['ESTIMATED_EARNINGS', 'IMPRESSIONS', 'IMPRESSION_RPM']
            sort_conditions = {'dimension': 'DATE', 'order': 'DESCENDING'}
            report_spec = {'dateRange': date_range,
                        'dimensions': dimensions,
                        'metrics': metrics,
                        'sortConditions': [sort_conditions]}
            
            # Generate report
            for account in accounts.get('account'):

                # Generate report
                raw_report = api.network_report(account.get('publisherId'),report_spec)
                app_data = api.app_list(account.get('publisherId'))
                app_df = pd.DataFrame(to_plain_app(app_data))
                df = pd.DataFrame(to_plain_report(raw_report))
                if len(df) == 0:
                    result_df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/admob.csv', index=False)
                    break
                df = df.merge(app_df, left_on='APP', right_on='appId', how='inner')
                
                df = df[df["appStoreId"].isin(dict_game_id)]

                df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d")
                df["PLATFORM"] = df["PLATFORM"].str.lower()
                df["COUNTRY"] = df["COUNTRY"].str.upper().str.strip().apply(lambda x: x if x in dict_country_id else 'OTH')
                df['ESTIMATED_EARNINGS'] = 0.9902 * (df['ESTIMATED_EARNINGS'].astype('float')) / 1000000 
                df['IMPRESSIONS'] = df['IMPRESSIONS'].astype('int')
                df = df.groupby(['DATE','PLATFORM','COUNTRY','appStoreId'])[['ESTIMATED_EARNINGS','IMPRESSIONS']].sum().reset_index()
                name_dict = {
                    'DATE' : 'update_date', 
                    'PLATFORM' : 'platform', 
                    'COUNTRY': 'country', 
                    'appStoreId' : 'product_id',
                    'ESTIMATED_EARNINGS' : 'revenue',
                    'IMPRESSIONS' : 'impressions'
                    }
                df.rename(columns=name_dict, inplace=True)
                df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
                df["network"] = 'Admob'
                fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
                df.fillna(value=fill_values, inplace=True)
                df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]
                result_df= pd.concat([result_df,df], ignore_index=True)

                print(f"Complete Get Revenue Data of {date.date()} From Admob")

    except HttpError as e:
        # Get the HTTP status code from the exception
        status_code = e.resp.status
        content = e.content
        response_data = json.loads(content)
        # Access the "message" value
        message_value = response_data[0]["error"]["message"]
        s = f"{status_code}: {message_value}"
        print((f"FAILED Get Revenue Data of {date.date()} From Admob with error: {s} !!!!!!"))
        # asyncio.run(send_telegram_msg(f"FAILED Get Revenue Data of {date.date()} From Admob with error: {s} !", '-1001962610175'))
    
    if(len(result_df)) > 0:
        result_df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/admob.csv', index=False)   
    
            
    print(f"End data Admob in: {datetime.now() - now}")

    
def etl_admob_rev_nocountry(today, yesterday):
    try:
        country_set = Country.objects.all()
        game_set = Game.objects.all()
        dict_game_id = {game.id_bundle: game for game in game_set}
        dict_country_id = {country.country_id: country for country in country_set}

        now = datetime.now()

        api = AdMobAPI()

        # Get my account
        accounts = api.accounts()
        # print(accounts)
         
        # Prepare report spec for the week
        date_range = {'startDate': {'year': yesterday.year, 'month': yesterday.month, 'day': yesterday.day},
                    'endDate': {'year': today.year, 'month': today.month, 'day': today.day}}
        dimensions = ['DATE', 'APP', 'PLATFORM']
        metrics = ['ESTIMATED_EARNINGS', 'IMPRESSIONS', 'IMPRESSION_RPM']
        sort_conditions = {'dimension': 'DATE', 'order': 'DESCENDING'}
        report_spec = {'dateRange': date_range,
                    'dimensions': dimensions,
                    'metrics': metrics,
                    'sortConditions': [sort_conditions]}

        # Generate report
        for account in accounts.get('account'):
            # # Generate report
            raw_report = api.network_report(account.get('publisherId'),report_spec)
            app_data = api.app_list(account.get('publisherId'))
            app_df = pd.DataFrame(to_plain_app(app_data))
            df = pd.DataFrame(to_plain_report(raw_report))
            if len(df) == 0:
                break
            df = df.merge(app_df, left_on='APP', right_on='appId', how='inner')
            df = df[df["appStoreId"].isin(dict_game_id)]
            df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d")
            df["PLATFORM"] = df["PLATFORM"].str.lower()
            df['ESTIMATED_EARNINGS'] = 0.9902 * (df['ESTIMATED_EARNINGS'].astype('float')) / 1000000 
            df['IMPRESSIONS'] = df['IMPRESSIONS'].astype('int')
            df = df.groupby(['DATE','PLATFORM','appStoreId'])[['ESTIMATED_EARNINGS','IMPRESSIONS']].sum().reset_index()
            name_dict = {
                'DATE' : 'update_date', 
                'PLATFORM' : 'platform', 
                'appStoreId' : 'product_id',
                'ESTIMATED_EARNINGS' : 'revenue',
                'IMPRESSIONS' : 'impressions'
                }
            df.rename(columns=name_dict, inplace=True)
            df["ecpm"] = np.where(df["impressions"]==0, 0, (df["revenue"]/df["impressions"])*1000)
            df["network"] = 'Admob'
            fill_values = {'impressions': 0, 'revenue': 0, 'ecpm': 0}
            df.fillna(value=fill_values, inplace=True)
            df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]

            df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/admob.csv', index=False)

            print(f"End data Admob NoCountry in: {datetime.now() - now}")

    except HttpError as e:
        # Get the HTTP status code from the exception
        status_code = e.resp.status
        content = e.content
        response_data = json.loads(content)
        # Access the "message" value
        # message_value = response_data[0]["error"]["message"]
        s = f"{status_code}: {response_data}"
        print(s)

def run_admob():
    today = datetime.now()
    yesterday = today - timedelta(days=2)
    etl_admob_rev(today, yesterday)
    etl_admob_rev_nocountry(today, yesterday)

