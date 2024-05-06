import sys
sys.path.insert(0, '/home/mkt-en/mkt_dashboard')
import os
import django
# Configure Django settings
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mkt_dashboard.settings")
django.setup()
from dashboard.models import Data_Appsflyer, Game, Country
import requests
import logging
import csv
import datetime, time
from datetime import datetime, timedelta, date
from bs4 import BeautifulSoup
from django.db import connection
from telegram_service import send_table
import asyncio
async def send_telegram_tbl(headers, data, msg, channel):
    await send_table(headers, data, msg, channel)
# logger = logging.getLogger('SmartfileTest')

# # create the handler for the main logger
# file_logger = logging.FileHandler('console.log')
# NEW_FORMAT = '[%(asctime)s] - [%(levelname)s]:\t %(filename)s: %(funcName)s(): %(lineno)d - %(message)s'
# file_logger_format = logging.Formatter(NEW_FORMAT)

# # tell the handler to use the above format
# file_logger.setFormatter(file_logger_format)

# # finally, add the handler to the base logger
# logger.addHandler(file_logger)

# # remember that by default, logging will start at 'warning' unless
# # we set it manually
# logger.setLevel(logging.DEBUG)

with open('/home/mkt-en/mkt_dashboard/conf/appsflyer.xml', 'r') as f:
    data = f.read()
config = BeautifulSoup(data, "xml")

class Warn_Appsflyer:
    current_date = date(2023, 10, 26)
    data_sent = []

def load_cost(FROM_DATE, TO_DATE):
    API_TOKEN = config.key.string
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE `dashboard_data_appsflyer`")
    list_game = Game.objects.all()
    for data_game in list_game:
        update_database_appsflyer(data_game, API_TOKEN, FROM_DATE, TO_DATE, data_game.platform)


def update_database_appsflyer(GAME, API_TOKEN, FROM_DATE, TO_DATE, PLATFORM):
    if GAME.platform == 'ios':
        app_id = 'id' + GAME.id_bundle
    else:
        app_id = GAME.id_bundle
    report_type = 'geo_by_date_report'
    params_appsflyer = {
        'from': FROM_DATE.strftime("%Y-%m-%d"),
        'to': TO_DATE.strftime("%Y-%m-%d")
    }
    headers = {
        "accept": "text/csv",
        "authorization": ("Bearer " + str(API_TOKEN)).replace("\'","")
    }
    request_url_appsflyer = 'https://hq1.appsflyer.com/api/agg-data/export/app/{}/{}/v5'.format(app_id, report_type)
    print(datetime.now(), GAME.id_bundle, PLATFORM)
    network = {}
    list_country = Country.objects.all()
    dict_country = {}
    for x in list_country:
        dict_country.update({x.country_id: x})
    dict_network = {
        'applovin_int':'Applovin',
        'mintegral_int':'Mintegral',
        'ironsource_int':'Ironsource',
        'googleadwords_int':'Google Adwords',
        'Organic':'Organic',
        'unityads_int':'Unity',
        'bytedanceglobal_int':'Pangle',
        'shareit_int':'Share It',
        'xiaomiglobal_int':'Xiaomi Global',
        'aura_int':'Aura',
        'Apple Search Ads':'Apple Search Ads',
        'Facebook Ads':'FAN',
        'appnext_int':'App Next',
        'abi_promotion':'ABI Promotion',
        'tapjoy_int':'Tapjoy',
        'adjoe_int':'Adjoe',
        'mobavenue_int':'Mobavenue',
        'mocaglobal_int':'Moca Global',
        'taurus_int':'Taurus',
        'vungle_int':'Vungle',
        'SocialFacebook':'Social Facebook',
        'MegaJoy Xpromo':'MegaJoy Xpromo',
        'Social_facebook':'Social Facebook',
    }
    res_appsflyer = requests.request('GET', request_url_appsflyer, params=params_appsflyer, headers=headers)
    if res_appsflyer.status_code != 200:
        if res_appsflyer.status_code == 403:
            time.sleep(120)
            update_database_appsflyer(GAME, API_TOKEN, FROM_DATE, TO_DATE, PLATFORM)
        else:
            logging.debug('Appsflyer: ' + str(res_appsflyer.status_code) + " " + str(request_url_appsflyer))
            return
    else:
        form_data = res_appsflyer.content.decode('utf-8')
        if len(form_data) < 1:
            logging.debug(app_id + ' not have data in Appsflyer.')
            return
        csv_appsflyer = csv.reader( (line.replace('\0','') for line in  form_data.splitlines()), delimiter=',')
        next(csv_appsflyer)
        try:
            csv_list_appsflyer = list(csv_appsflyer)
        except :
            logging.error("Fail update cost", exc_info=True)
            return
        if len(csv_list_appsflyer) < 1:
            logging.debug(app_id + ' have empty data in Appsflyer, Date: ' + str(FROM_DATE))
            return
        elif len(csv_list_appsflyer) > 199999:
            logging.error(app_id + ' have too much data in Appsflyer, Date: ' + str(FROM_DATE), str(TO_DATE))
            return
        data_appsflyer = []
        i = 0
        old_len = 0
        time_add = 1
        num_max = len(csv_list_appsflyer[0])
        
        for line in csv_list_appsflyer: 
            if len(line) < num_max:
                continue
            data = Data_Appsflyer(product = GAME, platform=PLATFORM, uninstall_rate = 0)
            data.date_update = datetime.strptime(line[0 - old_len], '%Y-%m-%d' )
            if line[1 - old_len] in dict_country:
                data.country_id = line[1 - old_len]
            elif line[1 - old_len] == 'UK':
                data.country_id = 'GB'
            else:
                data.country_id = 'OTH'
            datanet = line[3 - old_len].encode('ascii', errors='ignore').decode('utf8')
            if datanet in dict_network:
                data.network = dict_network[datanet]
            else:
                data.network = 'Other'
            if line[8 - old_len] != 'N/A':
                data.installs = int(line[8 - old_len])
            else:
                data.installs = 0
            if line[13 - old_len] != 'N/A':
                data.activity_revenue = line[13 - old_len]
            else:
                data.activity_revenue = 0
            if line[14 - old_len] != 'N/A':
                data.cost = float(line[14 - old_len])
                if data.network not in network:
                    network[data.network] = float(data.cost)
                else:
                    network[data.network] += float(data.cost)
            else:
                data.cost = 0
            if line[15 - old_len] != 'N/A':
                data.roi = line[15 - old_len]
            else:
                data.roi = 0
            if line[17 - old_len] != 'N/A':
                data.avg_ecpi = line[17 - old_len]
            else:
                data.avg_ecpi = 0
            if data.cost == 0 and data.installs == 0:
                continue
            data_appsflyer.append(data)
            i=i+1
            if i > 5000:
                Data_Appsflyer.objects.bulk_create(data_appsflyer)
                data_appsflyer = []
                i = 0
        Data_Appsflyer.objects.bulk_create(data_appsflyer)
    temp_sent = send_cost_each(GAME.id_bundle)
    if len(temp_sent) > 0 and temp_sent not in Warn_Appsflyer.data_sent:
        Warn_Appsflyer.data_sent.append(temp_sent)
        day = str(date.today())
        day_b = str(date.today() - timedelta(days = 1))
        asyncio.run(send_telegram_tbl(["Game Cost", "Profit",day, day_b, "Percent"], temp_sent,'msg','-1001644266039'))
        asyncio.run(send_telegram_tbl(["Game Cost", "Profit",day, day_b, "Percent"], temp_sent,'msg', '-1001962610175'))
    return

def run_appsflyer():
    if date.today() != Warn_Appsflyer.current_date:
        Warn_Appsflyer.data_sent = []
        Warn_Appsflyer.current_date = date.today()
        print ("Reset warned data")
    today =datetime.now()
    yesterday= today - timedelta(days=2)
    load_cost(yesterday, today)
    
def send_cost_each(id_game):
    c = connection.cursor()
    try:
        c.callproc('warn_cost_each',(id_game, ))
        data_all = c.fetchall()
    except (AttributeError, OperationalError):
        print("Fail Send cost each", str(datetime.now()), id_game)
        connection.connect()
        c = connection.cursor()
        c.callproc('warn_cost_each',(id_game, ))
        data_all = c.fetchall()
    finally:
        c.close()
    return data_all

