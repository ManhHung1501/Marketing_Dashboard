import xml.etree.ElementTree as ET


def read_xml(net_name):
    # Load the XML file
    tree = ET.parse('/home/mkt-en/mkt_dashboard/conf/network.xml')  
    # Get the root element
    root = tree.getroot()
    target_net = next((net for net in root.findall('net') if net.get('name') == net_name), None)

    if net_name == 'InMobi':
        username = target_net.find('username').text
        key = target_net.find('key').text
        return username, key

    elif net_name in ["Vungle","AdColony","Applovin"]:
        key = target_net.find('key').text
        return key

    elif net_name == 'FAN':
        business_id = target_net.find('id').text
        key = target_net.find('key').text
        return business_id, key

    elif net_name == 'Ironsource':
        key = target_net.find('key').text
        token = target_net.find('token').text
        return key, token

    elif net_name == 'Fyber':
        secretkey = target_net.find('secretkey').text
        apikey = target_net.find('apikey').text
        _id = target_net.find('id').text
        client_id = target_net.find('client_id').text
        client_secret = target_net.find('client_secret').text
        return secretkey, apikey, int(_id), client_id, client_secret
    
    elif net_name == 'Mintegral':
        secretkey = target_net.find('secretkey').text
        apikey = target_net.find('apikey').text
        access_key = target_net.find('access_key').text
        apikey_cost = target_net.find('api_key').text
        return secretkey, apikey, access_key, apikey_cost

    elif net_name == "Pangle":
        key = target_net.find('key').text
        role_id = target_net.find('id').text
        access_token = target_net.find('access_token').text
        return key, role_id, access_token
    
    elif net_name == "Unity":
        organize_id = target_net.find('id').text
        cost_id = target_net.find('sub_id').text
        secretkey = target_net.find('secretkey').text
        apikey = target_net.find('apikey').text
        refresh_token = target_net.find('refresh_token').text
        return organize_id, cost_id, secretkey, apikey, refresh_token

    elif net_name == "Admob":
        pub = target_net.find('pub').text
        return pub


class vungle:
    url_rev = "https://report.api.vungle.com/ext/pub/reports/performance"
    url_cost = "https://report.api.vungle.com/ext/adv/reports/spend"

    key = read_xml("Vungle")

    headers = {
        "Authorization": f"Bearer {key}",
        "Vungle-Version": "1",
        "Accept": "application/json"
    }
    
    params =  {
            "dimensions": "",
            "aggregates": "",
            "start": "",
            "end": ""
        }

class applovin:
    url = "https://r.applovin.com/report"
    url_max = "https://r.applovin.com/maxReport"
    key = read_xml("Applovin")
    
    params = {
        "api_key": key,
        "columns": "",
        "format": "json",
        "start": "",
        "end": ""
    }

class mintegral:
    url_rev = "https://api.mintegral.com/reporting/v2/data"
    url_cost = "https://ss-api.mintegral.com/api/v2/reports/data"

    secret_key, key, access_key, apikey_cost = read_xml("Mintegral")

    headers ={
        "access-key": "",
        "token": "",
        "timestamp": ""
    }

    params = {
        "skey": key,
        "sign": "",
        "timezone": "0",
        "time": "",
        "group_by": "",
        "start":"",
        "end":""
    }

    params_cost = {
        "timezone": "0",
        "dimension_option": "",
        "time_granularity": "",
        "start_time":"",
        "end_time":""
    }


class ironsource:
    url_rev = "https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats"
    url_cost = "https://api.ironsrc.com/advertisers/v4/reports/cost"

    secret_key, refreshToken = read_xml("Ironsource")

    headers = {"Authorization": ""}

    params ={
        "startDate": "",
        "endDate": "",
        "breakdowns": "",
        "metrics": ""
    }

class pangle:
    url_rev = "https://open-api.pangleglobal.com/union_pangle/open/api/rt/income"
    url_cost = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"

    secure_key, role_id, access_token = read_xml('Pangle') 

    params = {
        'user_id': role_id,
        'role_id': role_id,
        'version' : 2.0,
        'time_zone': 0,
        'currency': 'usd',
        'sign_type' : 'MD5',
        'sign': "",
        'date': "",
        'dimensions': ""
    }

    data = {
        "advertiser_id": "",
        "service_type": "",
        "report_type": "",
        "data_level": "",
        "dimensions": "",
        "metrics": "",
        "start_date": "2023-09-25",
        "end_date": "2023-09-26",
        "page_size": 1000
    }

    headers = {
        "Access-Token": access_token
    }

class adcolony:
    url_rev = "http://clients-api.adcolony.com/api/v2/publisher_summary"
    url_cost = "https://clients-api.adcolony.com/api/v2/advertiser_summary"

    key = read_xml("AdColony")

    params =  {
        "user_credentials": key,
        "format": "json",
        "date_group": "",
        "date": "",
        "end_date": ""
    }

class inmobi:
    url_rev = 'https://api.inmobi.com/v3.0/reporting/publisher'

    userName, secretKey = read_xml("InMobi")
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'accountId': '',
        'secretKey': secretKey,
        'sessionId': '',
    }

    params = {
        'reportRequest': {
            'metrics': [],
            'timeFrame': '',
            'groupBy': [],
            "offset": 1, 
            "length":5000
        }
    }


class fyber:
    oauth_signature,oauth_consumer_key,publisherId, client_id, client_secret = read_xml('Fyber')
    
    url_rev = "https://revenuedesk.fyber.com/iamp/services/performance/{}/vamp/{}/{}"

    headers = {
        "Content-type" : "application/json",
        "Accept": "application/json"
    }

    
class unity:
    organize_id, cost_id, secretkey, apikey, refresh_token = read_xml('Unity')

    url_rev = "https://gameads-admin.applifier.com/stats/monetization-api"
    url_cost = f"https://stats.unityads.unity3d.com/organizations/{cost_id}/reports/acquisitions"


    params = {
        "apikey": apikey,
        "fields": "",
        "splitBy": "",
        "scale": "",
        "start": ""
    }

class fan:
    business_id,token = read_xml("FAN")

    url_rev = f'https://graph.facebook.com/v18.0/{business_id}/adnetworkanalytics'

    params = {
        'metrics': [],
        'breakdowns': [],
        'since': '',
        'until': '',
        'ordering_column': "",
        'access_token': token
    }

    


