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
from googleads import ad_manager
from googleads import oauth2
from datetime import datetime, timedelta
import tempfile
import pandas as pd
import numpy as np


def categorize_device(device_name):
    if 'Apple' in device_name:
        return 'ios'
    else:
        return 'android'

def etl_gam_rev(today,yesterday):
    start_time = datetime.now()
    oauth2_client = oauth2.GoogleServiceAccountClient(
        "/home/mkt-en/mkt_dashboard/conf/service_account.json", oauth2.GetAPIScope('ad_manager'))
    ad_manager_client = ad_manager.AdManagerClient(oauth2_client, application_name= "ABI", network_code=22442147457)


    end_date = today.date()
    start_date = yesterday.date()
    
    # Define the report query
    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'MOBILE_APP_NAME', 'MOBILE_APP_RESOLVED_ID', 'MOBILE_DEVICE_NAME', 'COUNTRY_CODE'],
            'columns': ['AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS', 'AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE'],
            'dateRangeType': 'CUSTOM_DATE',
            'startDate': start_date,
            'endDate': end_date
        }
    }

    # Initialize the report downloader
    report_downloader = ad_manager_client.GetDataDownloader(version='v202308')

    # Run the report and wait for it to complete
    report_job_id = report_downloader.WaitForReport(report_job)

    # Once the report is completed, download the report data
    export_format = 'CSV_DUMP'
    report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
    report_downloader.DownloadReportToFile(report_job_id, export_format, report_file)
    report_file.close()
    print(f'Request Gam Success in: {datetime.now() - start_time}')

    # Transform data
    country_set = Country.objects.all()
    game_set = Game.objects.all()
    network_set = Network.objects.all()
    dict_game_id = {game.id_bundle: game for game in game_set}
    dict_country_id = {country.country_id: country for country in country_set}
    dict_network = {network.name: network for network in network_set}
    
    start_time = datetime.now()
    df = pd.read_csv(report_file.name)
    df = df[df["Dimension.MOBILE_APP_RESOLVED_ID"]!='(Not applicable)']
    df['platform'] = df['Dimension.MOBILE_DEVICE_NAME'].apply(categorize_device)
    df["Dimension.MOBILE_APP_RESOLVED_ID"] = df["Dimension.MOBILE_APP_RESOLVED_ID"].str.strip()
    df = df[df["Dimension.MOBILE_APP_RESOLVED_ID"].isin(dict_game_id)]
    df["Dimension.COUNTRY_CODE"] = df["Dimension.COUNTRY_CODE"].str.upper().str.strip().apply(lambda x: x if x in dict_country_id else 'OTH')
    df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE"] =df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE"].astype('float')
    df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS"] = df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS"].astype(int)
    df['revenue'] = df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE"] / 1000000 * 0.97
    df = df.groupby(['Dimension.DATE','platform','Dimension.COUNTRY_CODE','Dimension.MOBILE_APP_RESOLVED_ID'])[['revenue','Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS']].sum().reset_index()
    df['ecpm'] = np.where(df['Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS'] == 0, 0, (df['revenue'] / df['Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS'])*1000)
    fill_values = {'Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS': 0, 'revenue': 0, 'ecpm': 0}
    df.fillna(value=fill_values, inplace=True)
    df['ecpm'] = df["ecpm"].astype("float")
    name_dict = {
            'Dimension.DATE' : 'update_date', 
            'platform' : 'platform', 
            'Dimension.COUNTRY_CODE': 'country', 
            'Dimension.MOBILE_APP_RESOLVED_ID' : 'product_id',
            'revenue' : 'revenue',
            'Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS' : 'impressions',
            'ecpm' : 'ecpm'
            }
    df['network'] = 'GAM'
    df.rename(columns = name_dict, inplace  = True)
    df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]
    df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country/gam.csv', index = False)
    print(f'Transform Gam Success in: {datetime.now() - start_time}')

def etl_gam_rev_nocountry(today,yesterday):
    start_time = datetime.now()
    oauth2_client = oauth2.GoogleServiceAccountClient(
        "/home/mkt-en/mkt_dashboard/conf/service_account.json", oauth2.GetAPIScope('ad_manager'))
    ad_manager_client = ad_manager.AdManagerClient(oauth2_client, application_name= "ABI", network_code=22442147457)
    
    end_date = today.date()
    start_date = yesterday.date()
    
    # Define the report query
    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'MOBILE_APP_NAME', 'MOBILE_APP_RESOLVED_ID', 'MOBILE_DEVICE_NAME'],
            'columns': ['AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS', 'AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE'],
            'dateRangeType': 'CUSTOM_DATE',
            'startDate': start_date,
            'endDate': end_date
        }
    }


    # Initialize the report downloader
    report_downloader = ad_manager_client.GetDataDownloader(version='v202308')

    # Run the report and wait for it to complete
    report_job_id = report_downloader.WaitForReport(report_job)

    # Once the report is completed, download the report data
    export_format = 'CSV_DUMP'
    report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
    report_downloader.DownloadReportToFile(report_job_id, export_format, report_file)
    report_file.close()
    print(f'Request Gam Success in: {datetime.now() - start_time}')

    # Transform data
    start_time = datetime.now()
    game_set = Game.objects.all()
    network_set = Network.objects.all()
    dict_game_id = {game.id_bundle: game for game in game_set}
    dict_network = {network.name: network for network in network_set}
    

    df = pd.read_csv(report_file.name)
    df = df[df["Dimension.MOBILE_APP_RESOLVED_ID"]!='(Not applicable)']
    df['platform'] = df['Dimension.MOBILE_DEVICE_NAME'].apply(categorize_device)
    df["Dimension.MOBILE_APP_RESOLVED_ID"] = df["Dimension.MOBILE_APP_RESOLVED_ID"].str.strip()
    df = df[df["Dimension.MOBILE_APP_RESOLVED_ID"].isin(dict_game_id)]
    df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS"] = df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS"].astype(int)
    df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE"] =df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE"].astype('float')
    df['revenue'] = df["Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE"] / 1000000 * 0.97
    df = df.groupby(['Dimension.DATE','platform','Dimension.MOBILE_APP_RESOLVED_ID'])[['revenue','Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS']].sum().reset_index()
    df['ecpm'] = np.where(df['Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS'] == 0, 0, (df['revenue'] / df['Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS'])*1000)
    fill_values = {'Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS': 0, 'revenue': 0, 'ecpm': 0}
    df.fillna(value=fill_values, inplace=True)
    df['ecpm'] = df["ecpm"].astype("float")
    name_dict = {
        'Dimension.DATE' : 'update_date', 
        'platform' : 'platform', 
        'Dimension.MOBILE_APP_RESOLVED_ID' : 'product_id',
        'revenue' : 'revenue',
        'Column.AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS' : 'impressions',
        'ecpm' : 'ecpm'
        }
    df['network'] = 'GAM'
    df.rename(columns = name_dict, inplace  = True)
    df = df[(df["impressions"] != 0) | (df["revenue"] != 0)]
    df.to_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country/gam.csv', index = False)
    print(f'Transform Gam NOCOUNTRY Success in: {datetime.now() - start_time}')
    
   

def run_gam():
    today = datetime.now() 
    yesterday = today - timedelta(days=2)
    etl_gam_rev(today,yesterday)
    etl_gam_rev_nocountry(today,yesterday)

