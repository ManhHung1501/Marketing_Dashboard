from requests.structures import CaseInsensitiveDict
import requests
import csv, json
from bs4 import BeautifulSoup
from dashboard.models import Game, Network

with open('/home/mkt-en/mkt_dashboard/conf/network.xml', 'r') as f:
    data = f.read()

def get_app_nameGG(package_name, name_default):
    url = f'https://play.google.com/store/apps/details?id={package_name}'
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            app_name = soup.find('h1', {'itemprop': 'name'}).text
            return app_name
        else:
            return name_default
    except Exception as e:
        return name_default

def get_app_nameAP(track_id, name_default):
    base_url = "https://itunes.apple.com/lookup"
    params = {"id": track_id}
    
    try:
        response = requests.get(base_url, params=params)
        data = response.json()
        
        if data["resultCount"] > 0:
            app_name = data["results"][0]["trackName"]
            return app_name
        else:
            return name_default
    except Exception as e:
        return name_default

def update_app():
    config = BeautifulSoup(data, "xml").find('networks').find('net', {'name':'Ironsource'})
    SECRET_KEY = config.key.string
    REFRESH_TOKEN = config.token.string
    auth_url = 'https://platform.ironsrc.com/partners/publisher/auth'
    auth_headers = {
        'secretkey': str(SECRET_KEY),
        'refreshToken': str(REFRESH_TOKEN),
    }
    res_auth = requests.request('GET', auth_url,headers=auth_headers)
    if res_auth.status_code != 200:
        self.stdout.write("Error Auth: ", SECRET_KEY)
        return
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = "Bearer " + res_auth.text.removeprefix('"')
    request_app = "https://platform.ironsrc.com/partners/publisher/applications/v6?"
    res_app = requests.get(request_app, headers=headers)
    if res_app.status_code != 200:
        self.stdout.write("Error Request")
        return
    app_list_json = res_app.json()
    all_game = Game.objects.all()
    dict_game_id = {}
    for game in all_game:
        dict_game_id.update({game.id_bundle:game})
    all_network = Network.objects.all()
    dict_source = {}
    for x in all_network:
        dict_source.update({x.name:x})
    dict_network = {'Cross Promotion':'Ironsource','Fyber':'Fyber','Google AdMob':'Admob',
                    'Meta':'FAN','Pangle':'Pangle','Google AdManager':'GAM', 'ironSource':'Ironsource',
                    'Mintegral':'Mintegral','UnityAds':'Unity','Vungle':'Vungle','InMobi':'InMobi', 'AdColony':'AdColony',
                    'AppLovin':'Applovin', 'TapJoy':'TapJoy','AdFly':'AdFly','Direct Deals':'Direct Deals',
                    'CrossPromotion':'Ironsource', 'Liftoff Monetize':'Vungle', 'DT Exchange':'Fyber', 'Google AdManager Native':'GAM', 'Google AdMob Native':'Admob' }
    lost_net = []
    for obj in app_list_json:
        if obj['platform'] == 'Android':
            if obj['appStatus'] == 'active' and obj['bundleId'] and obj['bundleId'].strip():
                game_store = get_app_nameGG(obj['bundleId'], obj['appName'])
                if obj['bundleId'] not in dict_game_id:
                    new_game = Game(name=game_store,id_bundle=obj['bundleId'],platform='android')
                    new_game.save()
                    print("Ironsource Insert New Game: " + new_game.name)
                    dict_game_id.update({new_game.id_bundle:new_game})
                if dict_game_id[obj['bundleId']].name != game_store:
                    detail_game = dict_game_id[obj['bundleId']]
                    detail_game.name = game_store
                    detail_game.save()
        elif obj['platform'] == 'iOS':
            if obj['appStatus'] == 'active' and obj['trackId'] and obj['trackId'].strip():
                game_store = get_app_nameAP(obj['trackId'], obj['appName'])
                if obj['trackId'] not in dict_game_id:
                    new_game = Game(name=game_store,id_bundle=obj['trackId'],platform='ios', id_track=obj['bundleId'])
                    new_game.save()
                    print("Ironsource Insert New Game: " + new_game.name)
                    dict_game_id.update({new_game.id_bundle:new_game})
                if dict_game_id[obj['trackId']].name != game_store:
                    detail_game = dict_game_id[obj['trackId']]
                    detail_game.name = game_store
                    detail_game.save()
            if obj['appStatus'] == 'active' and obj['bundleId'] and obj['bundleId'].strip():
                detail_game = dict_game_id[obj['trackId']]
                if not detail_game.id_track:
                    detail_game.id_track = obj['bundleId']
                    detail_game.save()
                    print("Ironsource Insert Track Game: " + detail_game.name)

        for net in obj["networkReportingApi"]:
            if (net not in dict_network) and (net not in lost_net):
                lost_net.append(net)
    print("Lost Net: ", lost_net)