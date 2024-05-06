import sys
import os
sys.path.insert(0, '/home/mkt-en/mkt_dashboard')
import django
# Configure Django settings
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mkt_dashboard.settings")
django.setup()
import io
import zipfile
from google.cloud import storage
import pandas as pd
import xml.etree.ElementTree as ET
import numpy as np
from datetime import datetime,timedelta
from dashboard.models import Exchange,Game,Country
from dashboard.management.services import exchange
from google.api_core.exceptions import GoogleAPIError
from dashboard.management.services.telegram_service import send_msg
import asyncio

# Parse the XML file
tree = ET.parse('/home/mkt-en/mkt_dashboard/conf/google.xml')
root = tree.getroot()


def process_blob(blob):
    # Create an in-memory buffer to store the blob content
    buffer = io.BytesIO()

    # Download the blob's content into the buffer
    blob.download_to_file(buffer)
    
    # Move the buffer's file position to the beginning
    buffer.seek(0)

    # Extract the zip file
    with zipfile.ZipFile(buffer, 'r') as zip_ref:
        # Assuming there's only one file inside the zip
        file_name = zip_ref.namelist()[0]

        # Read the file inside the zip into a DataFrame
        with zip_ref.open(file_name) as file_in_zip:
            # You may need to specify the appropriate encoding and delimiter
            # depending on the format of the file inside the zip.
            df = pd.read_csv(file_in_zip, encoding='utf-8', delimiter=',', keep_default_na = False)
    return df

def list_blobs(bucket_name, blob_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return process_blob(blob)


def get_google_data_daily(Datatimerange):
    data_time = Datatimerange.strftime("%Y%m")
    final_data = pd.DataFrame()
    for info in root.findall('.//info'):
        try:
            blob_name = f"sales/salesreport_{data_time}.zip"
            name = info.get('name')  # Get the 'name' attribute value
            project_id = info.find('project_id').text
            file = info.find('file').text
            bucket = info.find('bucket').text
            credential_path = file
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
            data = list_blobs(bucket, blob_name)
            final_data = pd.concat([data,final_data], ignore_index=True)
            final_data.replace("", np.nan, inplace=True)
            print(f'Complete get data from account {name}')
        except GoogleAPIError as e:
            print(f"IAP Data of account {name} get API error: {e}")
            continue
    return final_data

def get_google_data_monthly(today):
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    data_time = last_month.strftime("%Y%m")
    print(data_time)
    final_data = pd.DataFrame()
    for info in root.findall('.//info'):
        blob_name = f"earnings/earnings_{data_time}"
        name = info.get('name')  # Get the 'name' attribute value
        project_id = info.find('project_id').text
        file = info.find('file').text
        bucket_name = info.find('bucket').text
        # bucket = 'pubsite_prod_rev_13619264998676921550'
        credential_path = file
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        # List objects in the bucket
        objects = list(bucket.list_blobs())
        # Extract object names
        object_names = [blob for blob in objects if blob_name in blob.name]
        for blob in object_names:
            data = process_blob(blob)
            final_data = pd.concat([data,final_data], ignore_index=True)
            final_data.replace("", np.nan, inplace=True)
    return final_data

def apply_conversion_daily(row,value):
    if row['Currency of Sale'] in value:
        charged_amount = str(row['Charged Amount']).replace(',', '')  # Remove commas
        charged_amount = float(charged_amount) 
        row['Charged Amount'] = (charged_amount / value[row['Currency of Sale']]) * 0.5985
    return row

def apply_conversion_monthly(row,value):
    if row['Merchant Currency'] in value:
        row['Amount (Merchant Currency)'] = (row['Amount (Merchant Currency)'] / value[row['Merchant Currency']]) * 0.96
    return row

def convert_time_format(time):
    time['Transaction Date'] = datetime.strptime(time['Transaction Date'], "%b %d, %Y")
    time['Transaction Date'] = time['Transaction Date'].strftime("%Y-%m-%d")
    return time

def strip_whitespace(value):
        return value.strip() if isinstance(value, str) else value

def etl_google_daily(start_date,end_date):
    list_date = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    final_data = pd.DataFrame()
    country_set = Country.objects.all()
    game_set = Game.objects.all()
    dict_game_id = {game.id_bundle: game for game in game_set}
    dict_country_id = {country.country_id: country for country in country_set}
    for date in list_date:
        start_etl_time = datetime.now()
        exchange.exchange_rate(date)
        currency_exchange = Exchange.objects.get(date_update = date)
        value = currency_exchange.rate
        try:
            data = get_google_data_daily(date)
        except GoogleAPIError as e:
            print(f"IAP Data of {date} is get API error: {e}")
            continue
        
        if len(data) == 0:
            print(f'Data google iap date {date} is not ready')
            return
        extract_time = datetime.now()
        print(f"Successfully extract IAP data from Google {date} - time: {datetime.now() - start_etl_time}")

        data['Order Charged Date'] = data['Order Charged Date'].astype(str)
        data = data[data['Order Charged Date'] == str(date)]
        data = data.apply(lambda x: x.map(strip_whitespace) if x.dtype == 'object' else x)
        data = data.apply(lambda x: apply_conversion_daily(x,value), axis=1)
        data['Country of Buyer'] = data['Country of Buyer'].str.upper().apply(lambda x: x if x in dict_country_id else 'OTH')
        data = data.groupby(['Order Charged Date','Country of Buyer','Product ID'])['Charged Amount'].sum().reset_index()
        data['platform'] = 'android'
        data = data[data['Product ID'].isin(dict_game_id)]
        data['Charged Amount'] = data['Charged Amount'].astype('float')
        name_dict = {
        'Order Charged Date' : 'update_date', 
        'Country of Buyer' : 'country',
        'platform' : 'platform', 
        'Product ID' : 'product_id',
        'Charged Amount' : 'revenue'
        }

        data.rename(columns = name_dict, inplace  = True)
        
        final_data = pd.concat([final_data,data], ignore_index=True)

    final_data= final_data[["update_date", "platform", "country", "product_id", "revenue"]]
    final_data = final_data.groupby(["update_date", "platform", "country", "product_id"])['revenue'].sum().reset_index()
    print(f"Successfully transform IAP data from Google - time: {datetime.now() - extract_time}")
    final_data.to_csv(f'/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/iap/google_daily.csv',index = False)

def etl_google_monthly(date_today):
    start_etl_time = datetime.now()
    today = date_today.date()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    month = last_month.strftime("%Y%m")
    country_set = Country.objects.all()
    game_set = Game.objects.all()
    dict_game_id = {game.id_bundle: game for game in game_set}
    dict_country_id = {country.country_id: country for country in country_set}
    exchange_rate = pd.DataFrame(Exchange.objects.all().values())
    exchange_rate["date_update"] = exchange_rate["date_update"].astype(str)
    data = get_google_data_monthly(date_today)
    extract_time = datetime.now()
    print(f"Successfully extract IAP data from Google month {month} - time: {datetime.now() - start_etl_time}")
    if len(data) == 0:
        print(f'No data for {month}')
        asyncio.run(send_msg(f"No data iap google monthly for {month} yet", '-1001962610175'))
        return 0
    data = data.apply(lambda x: convert_time_format(x),axis=1)
    data = data.apply(lambda x: x.map(strip_whitespace) if x.dtype == 'object' else x)
    data.rename(columns = {"Transaction Date" : "date_update"}, inplace = True)
    data = data.merge(exchange_rate,on = "date_update" ,how = "left")
    data = data.apply(lambda x: apply_conversion_monthly(x,x['rate']), axis=1)
    data = data.groupby(['date_update','Buyer Country','Product id'])['Amount (Merchant Currency)'].sum().reset_index()
    data['platform'] = 'android'
    data = data[data['Product id'].isin(dict_game_id)]
    name_dict = {
        'date_update' : 'update_date', 
        'Buyer Country' : 'country',
        'platform' : 'platform', 
        'Product id' : 'product_id',
        'Amount (Merchant Currency)' : 'revenue'
        }
    data.rename(columns = name_dict, inplace  = True)
    data= data[["update_date", "platform", "country", "product_id", "revenue"]]
    transform_time = datetime.now() - extract_time
    print(f"Successfully extract IAP data from Google - time: {transform_time}")
    data.to_csv(f'/home/mkt-en/mkt_dashboard/dashboard/etl/temp_data/iap/google_monthly.csv',index = False)

def run_google_iap():
    today = datetime.now().date()
    yesterday = today - timedelta(days=2)
    etl_google_daily(yesterday,today)
