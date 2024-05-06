import sys
sys.path.insert(0, '/home/mkt-en/mkt_dashboard')
import os
import django
# Configure Django settings
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mkt_dashboard.settings")
django.setup()
from django.db import connection
from mkt_dashboard.settings import DATABASES
from dashboard.models import Data_Appsflyer, DetailGame, TotalGame, Country, Game, Network
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from network_config import applovin
import logging


country_set = Country.objects.all()
game_set = Game.objects.all()
network_set = Network.objects.all()
dict_game_id = {game.id_bundle: game for game in game_set}
dict_country_id = {country.country_id: country for country in country_set}
dict_network = {network.name: network for network in network_set}


def update_detail_rev():
    data_path = '/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/split_country'
    for file_data in os.listdir(data_path):
        if file_data.endswith('.csv'):  
            file_data_path = os.path.join(data_path, file_data)
            now = datetime.now()

            # Read the file_data into a DataFrame
            df = pd.read_csv(file_data_path, keep_default_na = False)

            # Load data
            batch_size = 5000
            for i in range(0, len(df), batch_size):
                batch_data = df[i:i + batch_size]
                DetailGame.objects.bulk_create(
                    [
                    DetailGame(
                        date_update = item["update_date"],
                        product = dict_game_id.get(str(item["product_id"])),
                        country = dict_country_id.get(item["country"]),
                        network = dict_network.get(item["network"]),
                        iaa = item["revenue"],
                        impression = item["impressions"],
                        ecpm = item["ecpm"]
                    ) 
                    for index, item in batch_data.iterrows()
                    ],
                    update_conflicts = True,
                    update_fields=["iaa", "impression", "ecpm"]
                )
            
            print(f"Complete load {len(df)} from {file_data} to Detail Game in {datetime.now()- now}")

def update_total_rev():
    data_path = '/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/no_country'
    total_df = pd.DataFrame()

    now = datetime.now()
    print('Begin read file from Temp Folder')

    for file_data in os.listdir(data_path):
        if file_data.endswith('.csv'):  
            file_data_path = os.path.join(data_path, file_data)
            
            # Read the file_data into a DataFrame
            df = pd.read_csv(file_data_path, keep_default_na = False)
            df['revenue'] = df['revenue'].astype('float')
            df['impressions'] = df['impressions'].astype('int')

            total_df = pd.concat([total_df,df], ignore_index=True)
            
    total_row = len(total_df)

    # Filter  date
    total_df['update_date'] = pd.to_datetime(total_df['update_date']).dt.date
    today = datetime.today().date()
    yesterday = today - timedelta(days=2)
    total_df = total_df[(total_df['update_date'] >= yesterday) & (total_df['update_date'] <= today)]
    
    # Groupby all net
    total_df = total_df.groupby(['update_date','product_id'])[['revenue','impressions']].sum().reset_index()
    total_df["ecpm"] = np.where(total_df["impressions"]==0, 0, (total_df["revenue"]/total_df["impressions"])*1000)

    print(f'Read and Group By file from {total_row} rows into {len(total_df)} complete in: {datetime.now() - now}')
    

    # Load data
    now = datetime.now()
    batch_size = 5000
    for i in range(0, len(total_df), batch_size):
        batch_data = total_df[i:i + batch_size]
        TotalGame.objects.bulk_create(
            [
            TotalGame(
                date_update = item["update_date"],
                product = dict_game_id.get(str(item["product_id"])),
                iaa = item["revenue"],
                impression = item["impressions"],
                ecpm = item["ecpm"]
            ) 
            for index, item in batch_data.iterrows()
            ],
            update_conflicts = True,
            update_fields=["iaa", "impression", "ecpm"]
        )

    print(f'Load data to TotalGame Table complete in: {datetime.now() - now}')        

def update_iap():
    # apple_path = '/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/iap/apple.csv'
    # google_path = '/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/iap/google_daily.csv'

    start_time = datetime.now()
    apple_df = pd.read_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/iap/apple.csv', keep_default_na = False)
    google_daily = pd.read_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/iap/google_daily.csv', keep_default_na = False)

    df = pd.concat([google_daily, apple_df], ignore_index=True)
    print(f'Begin Update IAP with {len(df)} to Detail Game')
    
    # Load data
    batch_size = 5000
    for i in range(0, len(df), batch_size):
        batch_data = df[i:i + batch_size]
        DetailGame.objects.bulk_create(
            [
            DetailGame(
                date_update = item["update_date"],
                product = dict_game_id.get(str(item["product_id"])),
                country = dict_country_id.get(item["country"]),
                network = dict_network.get('Other'),
                iap = item["revenue"]
            ) 
            for index, item in batch_data.iterrows()
            ],
            update_conflicts = True,
            update_fields=["iap"]
        )

    print(f'Update Detail Game complete in: {datetime.now() - start_time}')
    

    # TOTAL GAME
    start_time = datetime.now()
    print('Begin Update IAP Total Game')

    df = df.groupby(['update_date','product_id'])['revenue'].sum().reset_index()

    # Load data
    load_time = datetime.now()
    batch_size = 5000
    for i in range(0, len(df), batch_size):
        batch_data = df[i:i + batch_size]
        TotalGame.objects.bulk_create(
            [
            TotalGame(
                date_update = item["update_date"],
                product = dict_game_id.get(str(item["product_id"])),
                iap = item["revenue"]
            ) 
            for index, item in batch_data.iterrows()
            ],
            update_conflicts = True,
            update_fields=["iap"]
        )

    print(f'Update Total Game complete in: {datetime.now() - start_time}')

def update_detail_cost():
    now = datetime.now()
    print('Begin get cost data from appsflyer temp table')

    df = pd.DataFrame(Data_Appsflyer.objects.all().values())
    rows = len(df)
    df = df.groupby(['date_update','country_id','product_id','network'])[['cost', 'installs']].sum().reset_index()
    df['cpi'] = np.where(df['installs']==0, 0, df['cost']/df['installs'])

    print(f'Complete get and group by cost data from {rows} rows into {len(df)} rows  in: {datetime.now() - now}')


    # Load data
    print('Begin load cost to DETAIL GAME table')
    now = datetime.now()

    batch_size = 5000
    for i in range(0, len(df), batch_size):
        batch_data = df[i:i + batch_size]
        DetailGame.objects.bulk_create(
            [
            DetailGame(
                date_update = item["date_update"],
                product = dict_game_id.get(str(item["product_id"])),
                country = dict_country_id.get(item["country_id"]),
                network = dict_network.get(item["network"]),
                cost = item["cost"],
                install = item["installs"],
                cpi = item["cpi"]
            ) 
            for index, item in batch_data.iterrows()
            ],
            update_conflicts = True,
            update_fields=["cost", "install", "cpi"]
        )

    print(f'Complete Load cost to DETAIL GAME Table in {datetime.now() - now}')

def update_total_cost():
    now = datetime.now()
    print('Begin get cost data from Appflyer temp table')

    df = pd.DataFrame(Data_Appsflyer.objects.all().values())
    rows = len(df)
    df = df.groupby(['date_update','product_id'])[['cost', 'installs']].sum().reset_index()
    df['cpi'] = np.where(df['installs']==0, 0, df['cost']/df['installs'])

    print(f'Complete get and group by cost data from {rows} rows into {len(df)} rows  in: {datetime.now() - now}')

    # Load data
    print('Begin load cost to TOTAL GAME table')
    now = datetime.now()

    batch_size = 5000
    for i in range(0, len(df), batch_size):
        batch_data = df[i:i + batch_size]
        TotalGame.objects.bulk_create(
            [
            TotalGame(
                date_update = item["date_update"],
                product = dict_game_id.get(str(item["product_id"])),
                cost = item["cost"],
                install = item["installs"],
                cpi = item["cpi"]
            ) 
            for index, item in batch_data.iterrows()
            ],
            update_conflicts = True,
            update_fields=["cost", "install", "cpi"]
        )

    print(f'Complete Load cost to TOTAL GAME Table in {datetime.now() - now}')


def update_iap_gg_monthly():
    start_time = datetime.now()
    df = pd.read_csv('/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/iap/google_monthly.csv', keep_default_na = False)

    print(f'Begin Update IAP with {len(df)} to Detail Game')
    
    # Load data
    batch_size = 5000
    for i in range(0, len(df), batch_size):
        batch_data = df[i:i + batch_size]
        DetailGame.objects.bulk_create(
            [
            DetailGame(
                date_update = item["update_date"],
                product = dict_game_id.get(str(item["product_id"])),
                country = dict_country_id.get(item["country"]),
                network = dict_network.get('Other'),
                iap = item["revenue"]
            ) 
            for index, item in batch_data.iterrows()
            ],
            update_conflicts = True,
            update_fields=["iap"]
        )

    print(f'Update Detail Game complete in: {datetime.now() - start_time}')
    

    # TOTAL GAME
    start_time = datetime.now()
    print('Begin Update IAP Total Game')

    df = df.groupby(['update_date','product_id'])['revenue'].sum().reset_index()

    # Load data
    batch_size = 5000
    for i in range(0, len(df), batch_size):
        batch_data = df[i:i + batch_size]
        TotalGame.objects.bulk_create(
            [
            TotalGame(
                date_update = item["update_date"],
                product = dict_game_id.get(str(item["product_id"])),
                iap = item["revenue"]
            ) 
            for index, item in batch_data.iterrows()
            ],
            update_conflicts = True,
            update_fields=["iap"]
        )

    print(f'Update Total Game complete in: {datetime.now() - start_time}')