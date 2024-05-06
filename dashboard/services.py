from .models import Game, Country, GameUser, Network, TotalGame, DetailGame
from django.contrib.auth.models import User
from django.core.cache import cache
from django.utils.datastructures import MultiValueDictKeyError
from datetime import date,timedelta, datetime
from django.db.models import Sum
def load_params(request):
    try:
        if request.user.get_username() in ["admin"]:
            all_game = Game.objects.all()
        else:
            all_game = GameUser.objects.get(user=request.user.get_username()).game.all()
    except Game.DoesNotExist:
        all_game = []
    try:
        if len(request.GET['include_partner']) > 0:
            select_partner = request.GET['include_partner']
            partners = select_partner.split(",")    
            print (partners)                
        else:
            select_partner = None
            partners = None
    except MultiValueDictKeyError:
        partners = None
    if partners == None:
        try:
            if len(request.GET['exclude_partner']) > 0:
                select_partner = request.GET['exclude_partner']
                partners = User.objects.exclude(username__in=select_partner.split(","))
            else:
                select_partner = None
                partners = None
        except MultiValueDictKeyError:
            select_partner = None
            partners = None
    cache.set('partners', partners)
    dict_game = {}
    for x in all_game:
        if partners:
            for y in GameUser.objects.filter(game=x._id):
                if y in partners:
                    dict_game.update({str(x._id):x.id_bundle})
                    break
        else:
            dict_game.update({str(x._id):x.id_bundle})
            
    try:
        game_name = []
        if len(request.GET['include_game_name']) > 0:
            select_game = request.GET['include_game_name']
            list_select_game = (select_game.replace('\'','')).split(",")
            for x in list_select_game:
                if x in dict_game:
                    game_name.append(dict_game[x])
        else:
            for x in all_game:
                game_name.append(x.id_bundle)
            select_game = None
    except MultiValueDictKeyError:
        game_name = None
    if game_name == None:
        game_name = []
        try:
            if len(request.GET['exclude_game_name']) > 0:
                select_game = request.GET['exclude_game_name']
                list_select_game = (select_game.replace('\'','')).split(",")
                for x in all_game:
                    if str(x._id) not in list_select_game:
                        game_name.append(x.id_bundle)
            else:
                for x in all_game:
                    game_name.append(x.id_bundle)
                select_game = None
        except MultiValueDictKeyError:
            for x in all_game:
                game_name.append(x.id_bundle)
            select_game = None
    try:
        platform = request.GET['platform']
    except MultiValueDictKeyError:
        platform = None      
    cache.set('platform', platform)
    try:
        if len(request.GET['include_country']) > 0:
            select_country = request.GET['include_country']
            countries = select_country.replace('\'','').split(",")
        else:
            select_country = None
            countries = None
    except MultiValueDictKeyError:
        countries = None
    if countries == None:
        countries = []
        try:
            if len(request.GET['exclude_country']) > 0:
                select_country = request.GET['exclude_country']
                list_country = Country.objects.all()
                list_unselect_country = select_country.replace('\'','').split(",")
                countries = []
                for x in list_country:
                    if x not in list_unselect_country:
                        countries.append(x.country_id)
            else:
                select_country = None
                countries = None
        except MultiValueDictKeyError:
            select_country = None
            countries = None
    cache.set('countries', countries)
    try:
        if len(request.GET['include_adnet']) > 0:
            select_adsource = request.GET['include_adnet']
            ad_source = select_adsource.split(",")
        else:
            select_adsource = None
            ad_source = None
    except MultiValueDictKeyError:
        ad_source = None
    if ad_source == None:
        ad_source = []
        try:
            if len(request.GET['exclude_adnet']) > 0:
                select_adsource = request.GET['exclude_adnet']
                list_network = Network.objects.all()
                list_unselect_network = select_adsource.split(",")
                ad_source = []
                for x in list_network:
                    if x not in list_unselect_network:
                        ad_source.append(x.name)
            else:
                select_adsource = None
                ad_source = None
        except MultiValueDictKeyError:
            select_adsource = None
            ad_source = None
    cache.set('adnet', ad_source)
    try:
        start_date = date.fromisoformat(request.GET['start_date'])
    except MultiValueDictKeyError:
        start_date = date.today() - timedelta(days = 1)
    try:
        end_date = date.fromisoformat(request.GET['end_date'])
    except MultiValueDictKeyError:
        end_date = date.today() - timedelta(days = 1)
    try:
        if len(request.GET['group_columns']) > 0:
            group_columns = request.GET['group_columns']
        else:
            group_columns = 'game_name'
    except MultiValueDictKeyError:
        group_columns = 'game_name'
    cache.set('group_columns', group_columns)
    return (game_name, platform, countries, ad_source, start_date, end_date, group_columns, partners, select_game, select_country, select_adsource, select_partner)

def query(request):
    (game_name, platform, countries, ad_source, start_date, end_date, group_columns, partners, select_game, select_country, select_adsource, select_partner) = load_params(request)
    print("Start calculate: " + str(datetime.now()))
    if request.user.get_username() == 'admin':
        list_game_model = Game.objects.all()
    else:
        list_game_model = GameUser.objects.get(user=request.user.get_username()).game.all()
    dict_game = {str(t.id_bundle):{'name': t.name,'id_bundle':t.id_bundle, 'id': t.id_bundle, 'company': "ABI", 'date': t.creation_date, 'platform':t.platform} for t in list_game_model}
    dict_country = {str(t.country_id):t.country_name for t in Country.objects.all()}
    list_params = group_columns.split(',')
    PARAMS = {'product':'Game','platform':'Platform','country_name':'Country','date_update':'Date'}
    json_data = {}
    json_data_total = {}
    data_sum = {}
    if group_columns == 'game_name':
        if select_country is None and select_adsource is None:
            data_total = TotalGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name).values_list('date_update','product_id','iaa','impression','ecpm','iap','cost','install','cpi')
        else:
            data_total = DetailGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name)
            if select_country and countries:
                data_total = data_total.filter(country_id__in = countries)
            if select_adsource and ad_source:
                data_total = data_total.filter(network__in = ad_source)
            data_total = data_total.values_list('date_update', 'product_id').annotate(sum_iaa=Sum('iaa'),sum_impression=Sum('impression'),sum_ecpm=Sum('iaa')/Sum('impression')*1000,sum_iap=Sum('iap'),sum_cost=Sum('cost'),sum_install=Sum('install'),sum_cpi=Sum('cost')/Sum('install'))
        for obj in list(data_total):
            game_id = str(obj[1])
            if game_id not in dict_game:
                continue
            if dict_game[str(obj[1])]['platform'] == 'android':
                link = 'https://play.google.com/store/apps/details?id=' + game_id
            else:
                link = 'https://apps.apple.com/us/app/apple-store/id' + game_id
            if (datetime.today().date() - dict_game[str(obj[1])]['date']).days < 10:
                isNew = True
            else:
                isNew = False
            if str(obj[1]) not in json_data:
                json_data.update({str(obj[1]):
                    {'game_id': obj[1],'game': dict_game[str(obj[1])]['name'], 
                    'platform': dict_game[str(obj[1])]['platform'], 
                    "cost": round(float(obj[6]),2), 
                    "subdata_installs": int(obj[7]), 
                    "subdata_activity_revenue": 0, 
                    "subdata_roi": round(0,2), 
                    "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
                    "subdata_uninstall_rate": round(0,2), 
                    "subdata_impression": int(obj[3]), 
                    "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
                    "revenue_in_app": round(float(obj[5]),2), 
                    "revenue_ads": round(float(obj[2]),2), 
                    "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
                    "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2), 
                    "roas": round((float(obj[5]) + float(obj[2])) / float(obj[6]),2) if float(obj[6]) > 0 else 0,
                    'link': link, 
                    'new': isNew, 
                    'data_by_date':[{"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(float(obj[5]),2),"revenue_sum":round(obj[5] + obj[2],2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)}]
                    }})
            else:
                json_data[str(obj[1])]["cost"] = round(json_data[str(obj[1])]["cost"] + float(obj[6]),2)
                json_data[str(obj[1])]["revenue_in_app"] = round(json_data[str(obj[1])]["revenue_in_app"] + obj[5],2)
                json_data[str(obj[1])]["revenue_ads"] = round(json_data[str(obj[1])]["revenue_ads"] + float(obj[2]),2)
                json_data[str(obj[1])]["revenue_sum"] = round(json_data[str(obj[1])]["revenue_sum"] + obj[5] + float(obj[2]),2)
                json_data[str(obj[1])]["profit"] = round(json_data[str(obj[1])]["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
                json_data[str(obj[1])]["roas"] = round(json_data[str(obj[1])]["revenue_sum"]/json_data[str(obj[1])]["cost"],2) if json_data[str(obj[1])]["cost"] > 0 else 0
                json_data[str(obj[1])]["subdata_installs"] += int(obj[7])
                json_data[str(obj[1])]["subdata_activity_revenue"] = 0
                json_data[str(obj[1])]["subdata_roi"] = 0
                json_data[str(obj[1])]["subdata_average_ecpi"] = round(json_data[str(obj[1])]["cost"]/json_data[str(obj[1])]["subdata_installs"],2) if json_data[str(obj[1])]["subdata_installs"] > 0 else 0
                json_data[str(obj[1])]["subdata_uninstall_rate"] = 0
                json_data[str(obj[1])]["subdata_impression"] += int(obj[3])
                json_data[str(obj[1])]["subdata_ecpm"] = round(json_data[str(obj[1])]["revenue_ads"] / json_data[str(obj[1])]["subdata_impression"] * 1000 if json_data[str(obj[1])]["subdata_impression"] > 0 else 0,2)
                json_data[str(obj[1])]["data_by_date"].append({"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(obj[5],2),"revenue_sum":round(obj[5] + float(obj[2]),2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)})
            if "cost" not in data_sum:
                data_sum.update(
                    {'game': "sum", 
                    'platform': '',"cost": round(float(obj[6]),2), 
                    "subdata_installs": int(obj[7]), 
                    "subdata_activity_revenue": 0, 
                    "subdata_roi": round(0,2), 
                    "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
                    "subdata_uninstall_rate": round(0,2), 
                    "subdata_impression": int(obj[3]), 
                    "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
                    "revenue_in_app": round(float(obj[5]),2), 
                    "revenue_ads": round(float(obj[2]),2), 
                    "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
                    "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2),
                    "roas": round((float(obj[5]) + float(obj[2])) / float(obj[6]),2) if float(obj[6]) > 0 else 0
                    })
            else:
                data_sum["cost"] = round(data_sum["cost"] + float(obj[6]),2)
                data_sum["revenue_in_app"] = round(data_sum["revenue_in_app"] + obj[5],2)
                data_sum["revenue_ads"] = round(data_sum["revenue_ads"] + float(obj[2]),2)
                data_sum["revenue_sum"] = round(data_sum["revenue_sum"] + obj[5] + float(obj[2]),2)
                data_sum["profit"] = round(data_sum["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
                data_sum["roas"] = round(data_sum["revenue_sum"]/data_sum["cost"],2) if data_sum["cost"] > 0 else 0
                data_sum["subdata_installs"] += int(obj[7])
                data_sum["subdata_activity_revenue"] = 0
                data_sum["subdata_roi"] = 0
                data_sum["subdata_average_ecpi"] = round(data_sum["cost"]/data_sum["subdata_installs"],2) if data_sum["subdata_installs"] > 0 else 0
                data_sum["subdata_uninstall_rate"] = 0
                data_sum["subdata_impression"] += int(obj[3])
                data_sum["subdata_ecpm"] = round(data_sum["revenue_ads"] / data_sum["subdata_impression"] * 1000 if data_sum["subdata_impression"] > 0 else 0,2)
            if str(obj[0]) not in json_data_total:
                json_data_total.update({str(obj[0]):{
                    "date":obj[0], "cost":float(obj[6]), "revenue_ads":float(obj[2]), "revenue_in_app":obj[5], "revenue_sum":obj[5] + float(obj[2]), "profit":float(obj[2]) + float(obj[5]) - float(obj[6])}
                })
            else:
                json_data_total[str(obj[0])]["cost"]  += float(obj[6])
                json_data_total[str(obj[0])]["revenue_ads"] += float(obj[2])
                json_data_total[str(obj[0])]["revenue_in_app"] += obj[5]
                json_data_total[str(obj[0])]["revenue_sum"] += obj[5] + float(obj[2]) 
                json_data_total[str(obj[0])]["profit"] += float(obj[2]) + float(obj[5]) - float(obj[6])
        data_sum.update({"data_by_date":list(json_data_total.values())})
    elif group_columns == 'date':
        if select_country is None and select_adsource is None:
            data_total = TotalGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name).values_list('date_update').annotate(iaa=Sum('iaa'),impression=Sum('impression'),ecpm=Sum('iaa')/Sum('impression')*1000,iap=Sum('iap'),cost=Sum('cost'),install=Sum('install'),cpi=Sum('cost')/Sum('install'))
        else:
            data_total = DetailGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name)
            if select_country and countries:
                data_total = data_total.filter(country_id__in = countries)
            if select_adsource and ad_source:
                data_total = data_total.filter(network__in = ad_source)
            data_total = data_total.values_list('date_update').annotate(sum_iaa=Sum('iaa'),sum_impression=Sum('impression'),sum_ecpm=Sum('iaa')/Sum('impression')*1000,sum_iap=Sum('iap'),sum_cost=Sum('cost'),sum_install=Sum('install'),sum_cpi=Sum('cost')/Sum('install'))
        for obj in list(data_total):
            if str(obj[0]) not in json_data:
                json_data.update({str(obj[0]):
                    {
                    "date":str(obj[0]),
                    "cost": round(float(obj[5]),2), 
                    "subdata_installs": int(obj[6]), 
                    "subdata_activity_revenue": 0, 
                    "subdata_roi": round(0,2), 
                    "subdata_average_ecpi": round(float(obj[5])/float(obj[6]),2) if float(obj[6]) > 0 else 0, 
                    "subdata_uninstall_rate": round(0,2), 
                    "subdata_impression": int(obj[2]), 
                    "subdata_ecpm": round(float(obj[1]) / int(obj[2]) * 1000 if int(obj[2]) > 0 else 0,2),
                    "revenue_in_app": round(float(obj[4]),2), 
                    "revenue_ads": round(float(obj[1]),2), 
                    "revenue_sum": round(float(obj[4]) + float(obj[1]),2), 
                    "profit": round(float(obj[4]) + float(obj[1]) - float(obj[5]),2),
                    "roas": round((float(obj[4]) + float(obj[1])) / float(obj[5]),2) if float(obj[5]) > 0 else 0,
                    'data_by_date':[{"date":obj[0],"cost":round(float(obj[5]),2),"revenue_ads":round(float(obj[1]),2),"revenue_in_app":round(float(obj[4]),2),"revenue_sum":round(obj[4] + obj[1],2),"profit":round(float(obj[1]) + float(obj[4]) - float(obj[5]),2)}]
                    }})
            else:
                json_data[str(obj[0])]["cost"] = round(json_data[str(obj[0])]["cost"] + float(obj[5]),2)
                json_data[str(obj[0])]["revenue_in_app"] = round(json_data[str(obj[0])]["revenue_in_app"] + obj[4],2)
                json_data[str(obj[0])]["revenue_ads"] = round(json_data[str(obj[0])]["revenue_ads"] + float(obj[1]),2)
                json_data[str(obj[0])]["revenue_sum"] = round(json_data[str(obj[0])]["revenue_sum"] + obj[4] + float(obj[1]),2)
                json_data[str(obj[0])]["profit"] = round(json_data[str(obj[0])]["profit"] + float(obj[1]) + float(obj[4]) - float(obj[5]),2)
                json_data[str(obj[0])]["roas"] = round(json_data[str(obj[0])]["revenue_sum"]/json_data[str(obj[0])]["cost"], 2) if json_data[str(obj[0])]["cost"] > 0 else 0
                json_data[str(obj[0])]["subdata_installs"] += int(obj[6])
                json_data[str(obj[0])]["subdata_activity_revenue"] = 0
                json_data[str(obj[0])]["subdata_roi"] = 0
                json_data[str(obj[0])]["subdata_average_ecpi"] = round(json_data[str(obj[0])]["cost"]/json_data[str(obj[0])]["subdata_installs"],2) if json_data[str(obj[0])]["subdata_installs"] > 0 else 0
                json_data[str(obj[0])]["subdata_uninstall_rate"] = 0
                json_data[str(obj[0])]["subdata_impression"] += int(obj[2])
                json_data[str(obj[0])]["subdata_ecpm"] = round(json_data[str(obj[0])]["revenue_ads"] / json_data[str(obj[0])]["subdata_impression"] * 1000 if json_data[str(obj[0])]["subdata_impression"] > 0 else 0,2)
                # json_data[str(obj[1])]["data_by_date"].append({"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(obj[5],2),"revenue_sum":round(obj[5] + float(obj[2]),2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)})
    elif group_columns == 'network':
        data_total = DetailGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name)
        if select_country and countries:
            data_total = data_total.filter(country_id__in = countries)
        if select_adsource and ad_source:
            data_total = data_total.filter(network__in = ad_source)
        data_total = data_total.values_list('date_update','network').annotate(sum_iaa=Sum('iaa'),sum_impression=Sum('impression'),sum_ecpm=Sum('iaa')/Sum('impression')*1000,sum_iap=Sum('iap'),sum_cost=Sum('cost'),sum_install=Sum('install'),sum_cpi=Sum('cost')/Sum('install'))
        for obj in list(data_total):
            if str(obj[1]) not in json_data:
                json_data.update({str(obj[1]):
                    {'network': obj[1],
                    "cost": round(float(obj[6]),2), 
                    "subdata_installs": int(obj[7]), 
                    "subdata_activity_revenue": 0, 
                    "subdata_roi": round(0,2), 
                    "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
                    "subdata_uninstall_rate": round(0,2), 
                    "subdata_impression": int(obj[3]), 
                    "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
                    "revenue_in_app": round(float(obj[5]),2), 
                    "revenue_ads": round(float(obj[2]),2), 
                    "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
                    "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2), 
                    "roas": round((float(obj[5]) + float(obj[2]))/float(obj[6]), 2),
                    'data_by_date':[{"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(float(obj[5]),2),"revenue_sum":round(obj[5] + obj[2],2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)}]
                    }})
            else:
                json_data[str(obj[1])]["cost"] = round(json_data[str(obj[1])]["cost"] + float(obj[6]),2)
                json_data[str(obj[1])]["revenue_in_app"] = round(json_data[str(obj[1])]["revenue_in_app"] + obj[5],2)
                json_data[str(obj[1])]["revenue_ads"] = round(json_data[str(obj[1])]["revenue_ads"] + float(obj[2]),2)
                json_data[str(obj[1])]["revenue_sum"] = round(json_data[str(obj[1])]["revenue_sum"] + obj[5] + float(obj[2]),2)
                json_data[str(obj[1])]["profit"] = round(json_data[str(obj[1])]["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
                json_data[str(obj[1])]["roas"] = round(json_data[str(obj[1])]["revenue_sum"] / json_data[str(obj[1])]["cost"],2) if json_data[str(obj[1])]["cost"] > 0 else 0
                json_data[str(obj[1])]["subdata_installs"] += int(obj[7])
                json_data[str(obj[1])]["subdata_activity_revenue"] = 0
                json_data[str(obj[1])]["subdata_roi"] = 0
                json_data[str(obj[1])]["subdata_average_ecpi"] = round(json_data[str(obj[1])]["cost"]/json_data[str(obj[1])]["subdata_installs"],2) if json_data[str(obj[1])]["subdata_installs"] > 0 else 0
                json_data[str(obj[1])]["subdata_uninstall_rate"] = 0
                json_data[str(obj[1])]["subdata_impression"] += int(obj[3])
                json_data[str(obj[1])]["subdata_ecpm"] = round(json_data[str(obj[1])]["revenue_ads"] / json_data[str(obj[1])]["subdata_impression"] * 1000 if json_data[str(obj[1])]["subdata_impression"] > 0 else 0,2)
                json_data[str(obj[1])]["data_by_date"].append({"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(obj[5],2),"revenue_sum":round(obj[5] + float(obj[2]),2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)})
            if "cost" not in data_sum:
                data_sum.update(
                    { 
                    'network': 'sum',"cost": round(float(obj[6]),2), 
                    "subdata_installs": int(obj[7]), 
                    "subdata_activity_revenue": 0, 
                    "subdata_roi": round(0,2), 
                    "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
                    "subdata_uninstall_rate": round(0,2), 
                    "subdata_impression": int(obj[3]), 
                    "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
                    "revenue_in_app": round(float(obj[5]),2), 
                    "revenue_ads": round(float(obj[2]),2), 
                    "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
                    "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2),
                    "roas": round((float(obj[5]) + float(obj[2]))/float(obj[6]), 2) if float(obj[6]) > 0 else 0
                    })
            else:
                data_sum["cost"] = round(data_sum["cost"] + float(obj[6]),2)
                data_sum["revenue_in_app"] = round(data_sum["revenue_in_app"] + obj[5],2)
                data_sum["revenue_ads"] = round(data_sum["revenue_ads"] + float(obj[2]),2)
                data_sum["revenue_sum"] = round(data_sum["revenue_sum"] + obj[5] + float(obj[2]),2)
                data_sum["profit"] = round(data_sum["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
                data_sum["roas"] = round(data_sum["revenue_sum"] / data_sum["cost"],2) if data_sum["cost"] > 0 else 0
                data_sum["subdata_installs"] += int(obj[7])
                data_sum["subdata_activity_revenue"] = 0
                data_sum["subdata_roi"] = 0
                data_sum["subdata_average_ecpi"] = round(data_sum["cost"]/data_sum["subdata_installs"],2) if data_sum["subdata_installs"] > 0 else 0
                data_sum["subdata_uninstall_rate"] = 0
                data_sum["subdata_impression"] += int(obj[3])
                data_sum["subdata_ecpm"] = round(data_sum["revenue_ads"] / data_sum["subdata_impression"] * 1000 if data_sum["subdata_impression"] > 0 else 0,2)
            if str(obj[0]) not in json_data_total:
                json_data_total.update({str(obj[0]):{
                    "date":obj[0], "cost":float(obj[6]), "revenue_ads":float(obj[2]), "revenue_in_app":obj[5], "revenue_sum":obj[5] + float(obj[2]), "profit":float(obj[2]) + float(obj[5]) - float(obj[6])}
                })
            else:
                json_data_total[str(obj[0])]["cost"]  += float(obj[6])
                json_data_total[str(obj[0])]["revenue_ads"] += float(obj[2])
                json_data_total[str(obj[0])]["revenue_in_app"] += obj[5]
                json_data_total[str(obj[0])]["revenue_sum"] += obj[5] + float(obj[2]) 
                json_data_total[str(obj[0])]["profit"] += float(obj[2]) + float(obj[5]) - float(obj[6])
        data_sum.update({"data_by_date":list(json_data_total.values())})
    elif group_columns == 'country_name':
        data_total = DetailGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name)
        if select_country and countries:
            data_total = data_total.filter(country_id__in = countries)
        if select_adsource and ad_source:
            data_total = data_total.filter(network__in = ad_source)
        data_total = data_total.values_list('date_update','country_id').annotate(sum_iaa=Sum('iaa'),sum_impression=Sum('impression'),sum_ecpm=Sum('iaa')/Sum('impression')*1000,sum_iap=Sum('iap'),sum_cost=Sum('cost'),sum_install=Sum('install'),sum_cpi=Sum('cost')/Sum('install'))
        for obj in list(data_total):
            if str(obj[1]) not in json_data:
                json_data.update({str(obj[1]):
                    {'country_code': obj[1], 'country': dict_country[str(obj[1])],
                    "cost": round(float(obj[6]),2), 
                    "subdata_installs": int(obj[7]), 
                    "subdata_activity_revenue": 0, 
                    "subdata_roi": round(0,2), 
                    "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
                    "subdata_uninstall_rate": round(0,2), 
                    "subdata_impression": int(obj[3]), 
                    "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
                    "revenue_in_app": round(float(obj[5]),2), 
                    "revenue_ads": round(float(obj[2]),2), 
                    "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
                    "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2),
                    "roas": round((float(obj[5]) + float(obj[2]))/float(obj[6]),2) if float(obj[6]) > 0 else 0,
                    'data_by_date':[{"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(float(obj[5]),2),"revenue_sum":round(obj[5] + obj[2],2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)}]
                    }})
            else:
                json_data[str(obj[1])]["cost"] = round(json_data[str(obj[1])]["cost"] + float(obj[6]),2)
                json_data[str(obj[1])]["revenue_in_app"] = round(json_data[str(obj[1])]["revenue_in_app"] + obj[5],2)
                json_data[str(obj[1])]["revenue_ads"] = round(json_data[str(obj[1])]["revenue_ads"] + float(obj[2]),2)
                json_data[str(obj[1])]["revenue_sum"] = round(json_data[str(obj[1])]["revenue_sum"] + obj[5] + float(obj[2]),2)
                json_data[str(obj[1])]["profit"] = round(json_data[str(obj[1])]["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
                json_data[str(obj[1])]["roas"] = round(json_data[str(obj[1])]["revenue_sum"] / json_data[str(obj[1])]["cost"],2) if json_data[str(obj[1])]["cost"] > 0 else 0
                json_data[str(obj[1])]["subdata_installs"] += int(obj[7])
                json_data[str(obj[1])]["subdata_activity_revenue"] = 0
                json_data[str(obj[1])]["subdata_roi"] = 0
                json_data[str(obj[1])]["subdata_average_ecpi"] = round(json_data[str(obj[1])]["cost"]/json_data[str(obj[1])]["subdata_installs"],2) if json_data[str(obj[1])]["subdata_installs"] > 0 else 0
                json_data[str(obj[1])]["subdata_uninstall_rate"] = 0
                json_data[str(obj[1])]["subdata_impression"] += int(obj[3])
                json_data[str(obj[1])]["subdata_ecpm"] = round(json_data[str(obj[1])]["revenue_ads"] / json_data[str(obj[1])]["subdata_impression"] * 1000 if json_data[str(obj[1])]["subdata_impression"] > 0 else 0,2)
                json_data[str(obj[1])]["data_by_date"].append({"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(obj[5],2),"revenue_sum":round(obj[5] + float(obj[2]),2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)})
            if "cost" not in data_sum:
                data_sum.update(
                    { 
                    'country': 'sum',"cost": round(float(obj[6]),2), 
                    "subdata_installs": int(obj[7]), 
                    "subdata_activity_revenue": 0, 
                    "subdata_roi": round(0,2), 
                    "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
                    "subdata_uninstall_rate": round(0,2), 
                    "subdata_impression": int(obj[3]), 
                    "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
                    "revenue_in_app": round(float(obj[5]),2), 
                    "revenue_ads": round(float(obj[2]),2), 
                    "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
                    "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2),
                    "roas": round((float(obj[5]) + float(obj[2]))/float(obj[6]),2)
                        })
            else:
                data_sum["cost"] = round(data_sum["cost"] + float(obj[6]),2)
                data_sum["revenue_in_app"] = round(data_sum["revenue_in_app"] + obj[5],2)
                data_sum["revenue_ads"] = round(data_sum["revenue_ads"] + float(obj[2]),2)
                data_sum["revenue_sum"] = round(data_sum["revenue_sum"] + obj[5] + float(obj[2]),2)
                data_sum["profit"] = round(data_sum["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
                data_sum["roas"] = round(data_sum["revenue_sum"] / data_sum["cost"],2) if data_sum["cost"] > 0 else 0
                data_sum["subdata_installs"] += int(obj[7])
                data_sum["subdata_activity_revenue"] = 0
                data_sum["subdata_roi"] = 0
                data_sum["subdata_average_ecpi"] = round(data_sum["cost"]/data_sum["subdata_installs"],2) if data_sum["subdata_installs"] > 0 else 0
                data_sum["subdata_uninstall_rate"] = 0
                data_sum["subdata_impression"] += int(obj[3])
                data_sum["subdata_ecpm"] = round(data_sum["revenue_ads"] / data_sum["subdata_impression"] * 1000 if data_sum["subdata_impression"] > 0 else 0,2)
            if str(obj[0]) not in json_data_total:
                json_data_total.update({str(obj[0]):{
                    "date":obj[0], "cost":float(obj[6]), "revenue_ads":float(obj[2]), "revenue_in_app":obj[5], "revenue_sum":obj[5] + float(obj[2]), "profit":float(obj[2]) + float(obj[5]) - float(obj[6])}
                })
            else:
                json_data_total[str(obj[0])]["cost"]  += float(obj[6])
                json_data_total[str(obj[0])]["revenue_ads"] += float(obj[2])
                json_data_total[str(obj[0])]["revenue_in_app"] += obj[5]
                json_data_total[str(obj[0])]["revenue_sum"] += obj[5] + float(obj[2]) 
                json_data_total[str(obj[0])]["profit"] += float(obj[2]) + float(obj[5]) - float(obj[6])
        data_sum.update({"data_by_date":list(json_data_total.values())})
    elif group_columns == 'platform':
        if select_country is None and select_adsource is None:
            data_total = TotalGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name).values_list('date_update','product_id','iaa','impression','ecpm','iap','cost','install','cpi')
        else:
            data_total = DetailGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name)
            if select_country and countries:
                data_total = data_total.filter(country_id__in = countries)
            if select_adsource and ad_source:
                data_total = data_total.filter(network__in = ad_source)
            data_total = data_total.values_list('date_update', 'product_id').annotate(sum_iaa=Sum('iaa'),sum_impression=Sum('impression'),sum_ecpm=Sum('iaa')/Sum('impression')*1000,sum_iap=Sum('iap'),sum_cost=Sum('cost'),sum_install=Sum('install'),sum_cpi=Sum('cost')/Sum('install'))
        for obj in list(data_total):
            game_id = str(obj[1])
            if game_id not in dict_game:
                continue
            key = dict_game[str(obj[1])]['platform']
            if key not in json_data:
                json_data.update({key:
                    {'platform': key, 
                    "cost": round(float(obj[6]),2), 
                    "subdata_installs": int(obj[7]), 
                    "subdata_activity_revenue": 0, 
                    "subdata_roi": round(0,2), 
                    "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
                    "subdata_uninstall_rate": round(0,2), 
                    "subdata_impression": int(obj[3]), 
                    "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
                    "revenue_in_app": round(float(obj[5]),2), 
                    "revenue_ads": round(float(obj[2]),2), 
                    "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
                    "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2), 
                    "roas": round((float(obj[5]) + float(obj[2])) / float(obj[6]),2) if float(obj[6]) > 0 else 0, 
                    'data_by_date':[{"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(float(obj[5]),2),"revenue_sum":round(obj[5] + obj[2],2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)}]
                    }})
            else:
                json_data[key]["cost"] = round(json_data[key]["cost"] + float(obj[6]),2)
                json_data[key]["revenue_in_app"] = round(json_data[key]["revenue_in_app"] + obj[5],2)
                json_data[key]["revenue_ads"] = round(json_data[key]["revenue_ads"] + float(obj[2]),2)
                json_data[key]["revenue_sum"] = round(json_data[key]["revenue_sum"] + obj[5] + float(obj[2]),2)
                json_data[key]["profit"] = round(json_data[key]["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
                json_data[key]["roas"] = round(json_data[key]["revenue_sum"] / json_data[key]["cost"],2) if json_data[key]["cost"] > 0 else 0
                json_data[key]["subdata_installs"] += int(obj[7])
                json_data[key]["subdata_activity_revenue"] = 0
                json_data[key]["subdata_roi"] = 0
                json_data[key]["subdata_average_ecpi"] = round(json_data[key]["cost"]/json_data[key]["subdata_installs"],2) if json_data[key]["subdata_installs"] > 0 else 0
                json_data[key]["subdata_uninstall_rate"] = 0
                json_data[key]["subdata_impression"] += int(obj[3])
                json_data[key]["subdata_ecpm"] = round(json_data[key]["revenue_ads"] / json_data[key]["subdata_impression"] * 1000 if json_data[key]["subdata_impression"] > 0 else 0,2)
                json_data[key]["data_by_date"].append({"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(obj[5],2),"revenue_sum":round(obj[5] + float(obj[2]),2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)})
            if "cost" not in data_sum:
                data_sum.update(
                    { 
                    'platform': 'sum',"cost": round(float(obj[6]),2), 
                    "subdata_installs": int(obj[7]), 
                    "subdata_activity_revenue": 0, 
                    "subdata_roi": round(0,2), 
                    "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
                    "subdata_uninstall_rate": round(0,2), 
                    "subdata_impression": int(obj[3]), 
                    "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
                    "revenue_in_app": round(float(obj[5]),2), 
                    "revenue_ads": round(float(obj[2]),2), 
                    "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
                    "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2),
                    "roas": round((float(obj[5]) + float(obj[2])) / float(obj[6]),2) if float(obj[6]) > 0 else 0
                    })
            else:
                data_sum["cost"] = round(data_sum["cost"] + float(obj[6]),2)
                data_sum["revenue_in_app"] = round(data_sum["revenue_in_app"] + obj[5],2)
                data_sum["revenue_ads"] = round(data_sum["revenue_ads"] + float(obj[2]),2)
                data_sum["revenue_sum"] = round(data_sum["revenue_sum"] + obj[5] + float(obj[2]),2)
                data_sum["profit"] = round(data_sum["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
                data_sum["roas"] = round(data_sum["revenue_sum"] / data_sum["cost"],2) if data_sum["cost"] > 0 else 0
                data_sum["subdata_installs"] += int(obj[7])
                data_sum["subdata_activity_revenue"] = 0
                data_sum["subdata_roi"] = 0
                data_sum["subdata_average_ecpi"] = round(data_sum["cost"]/data_sum["subdata_installs"],2) if data_sum["subdata_installs"] > 0 else 0
                data_sum["subdata_uninstall_rate"] = 0
                data_sum["subdata_impression"] += int(obj[3])
                data_sum["subdata_ecpm"] = round(data_sum["revenue_ads"] / data_sum["subdata_impression"] * 1000 if data_sum["subdata_impression"] > 0 else 0,2)
            if str(obj[0]) not in json_data_total:
                json_data_total.update({str(obj[0]):{
                    "date":obj[0], "cost":float(obj[6]), "revenue_ads":float(obj[2]), "revenue_in_app":obj[5], "revenue_sum":obj[5] + float(obj[2]), "profit":float(obj[2]) + float(obj[5]) - float(obj[6])}
                })
            else:
                json_data_total[str(obj[0])]["cost"]  += float(obj[6])
                json_data_total[str(obj[0])]["revenue_ads"] += float(obj[2])
                json_data_total[str(obj[0])]["revenue_in_app"] += obj[5]
                json_data_total[str(obj[0])]["revenue_sum"] += obj[5] + float(obj[2]) 
                json_data_total[str(obj[0])]["profit"] += float(obj[2]) + float(obj[5]) - float(obj[6])
        data_sum.update({"data_by_date":list(json_data_total.values())})
    # elif group_columns == 'company':
    #     if select_country is None and select_adsource is None:
    #         data_total = TotalGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name).values_list('date_update','product_id','iaa','impression','ecpm','iap','cost','install','cpi')
    #     else:
    #         data_total = DetailGame.objects.filter(date_update__gte=start_date, date_update__lte=end_date, product_id__in=game_name)
    #         if select_country and countries:
    #             data_total = data_total.filter(country_id__in = countries)
    #         if select_adsource and ad_source:
    #             data_total = data_total.filter(network__in = ad_source)
    #         data_total = data_total.values_list('date_update', 'product_id').annotate(sum_iaa=Sum('iaa'),sum_impression=Sum('impression'),sum_ecpm=Sum('iaa')/Sum('impression')*1000,sum_iap=Sum('iap'),sum_cost=Sum('cost'),sum_install=Sum('install'),sum_cpi=Sum('cost')/Sum('install'))
    #     for obj in list(data_total):
    #         game_id = str(obj[1])
    #         if game_id not in dict_game:
    #             continue
    #         key = dict_game[str(obj[1])]['platform']
    #         if key not in json_data:
    #             json_data.update({key:
    #                 {'platform': key, 
    #                 "cost": round(float(obj[6]),2), 
    #                 "subdata_installs": int(obj[7]), 
    #                 "subdata_activity_revenue": 0, 
    #                 "subdata_roi": round(0,2), 
    #                 "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
    #                 "subdata_uninstall_rate": round(0,2), 
    #                 "subdata_impression": int(obj[3]), 
    #                 "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
    #                 "revenue_in_app": round(float(obj[5]),2), 
    #                 "revenue_ads": round(float(obj[2]),2), 
    #                 "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
    #                 "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2), 
    #                 'data_by_date':[{"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(float(obj[5]),2),"revenue_sum":round(obj[5] + obj[2],2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)}]
    #                 }})
    #         else:
    #             json_data[key]["cost"] = round(json_data[key]["cost"] + float(obj[6]),2)
    #             json_data[key]["revenue_in_app"] = round(json_data[key]["revenue_in_app"] + obj[5],2)
    #             json_data[key]["revenue_ads"] = round(json_data[key]["revenue_ads"] + float(obj[2]),2)
    #             json_data[key]["revenue_sum"] = round(json_data[key]["revenue_sum"] + obj[5] + float(obj[2]),2)
    #             json_data[key]["profit"] = round(json_data[key]["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
    #             json_data[key]["subdata_installs"] += int(obj[7])
    #             json_data[key]["subdata_activity_revenue"] = 0
    #             json_data[key]["subdata_roi"] = 0
    #             json_data[key]["subdata_average_ecpi"] = round(json_data[key]["cost"]/json_data[key]["subdata_installs"],2) if json_data[key]["subdata_installs"] > 0 else 0
    #             json_data[key]["subdata_uninstall_rate"] = 0
    #             json_data[key]["subdata_impression"] += int(obj[3])
    #             json_data[key]["subdata_ecpm"] = round(json_data[key]["revenue_ads"] / json_data[key]["subdata_impression"] * 1000 if json_data[key]["subdata_impression"] > 0 else 0,2)
    #             json_data[key]["data_by_date"].append({"date":obj[0],"cost":round(float(obj[6]),2),"revenue_ads":round(float(obj[2]),2),"revenue_in_app":round(obj[5],2),"revenue_sum":round(obj[5] + float(obj[2]),2),"profit":round(float(obj[2]) + float(obj[5]) - float(obj[6]),2)})
    #         if "cost" not in data_sum:
    #             data_sum.update(
    #                 { 
    #                 'platform': 'sum',"cost": round(float(obj[6]),2), 
    #                 "subdata_installs": int(obj[7]), 
    #                 "subdata_activity_revenue": 0, 
    #                 "subdata_roi": round(0,2), 
    #                 "subdata_average_ecpi": round(float(obj[6])/float(obj[7]),2) if float(obj[7]) > 0 else 0, 
    #                 "subdata_uninstall_rate": round(0,2), 
    #                 "subdata_impression": int(obj[3]), 
    #                 "subdata_ecpm": round(float(obj[2]) / int(obj[3]) * 1000 if int(obj[3]) > 0 else 0,2),
    #                 "revenue_in_app": round(float(obj[5]),2), 
    #                 "revenue_ads": round(float(obj[2]),2), 
    #                 "revenue_sum": round(float(obj[5]) + float(obj[2]),2), 
    #                 "profit": round(float(obj[5]) + float(obj[2]) - float(obj[6]),2)})
    #         else:
    #             data_sum["cost"] = round(data_sum["cost"] + float(obj[6]),2)
    #             data_sum["revenue_in_app"] = round(data_sum["revenue_in_app"] + obj[5],2)
    #             data_sum["revenue_ads"] = round(data_sum["revenue_ads"] + float(obj[2]),2)
    #             data_sum["revenue_sum"] = round(data_sum["revenue_sum"] + obj[5] + float(obj[2]),2)
    #             data_sum["profit"] = round(data_sum["profit"] + float(obj[2]) + float(obj[5]) - float(obj[6]),2)
    #             data_sum["subdata_installs"] += int(obj[7])
    #             data_sum["subdata_activity_revenue"] = 0
    #             data_sum["subdata_roi"] = 0
    #             data_sum["subdata_average_ecpi"] = round(data_sum["cost"]/data_sum["subdata_installs"],2) if data_sum["subdata_installs"] > 0 else 0
    #             data_sum["subdata_uninstall_rate"] = 0
    #             data_sum["subdata_impression"] += int(obj[3])
    #             data_sum["subdata_ecpm"] = round(data_sum["revenue_ads"] / data_sum["subdata_impression"] * 1000 if data_sum["subdata_impression"] > 0 else 0,2)
    #         if str(obj[0]) not in json_data_total:
    #             json_data_total.update({str(obj[0]):{
    #                 "date":obj[0], "cost":float(obj[6]), "revenue_ads":float(obj[2]), "revenue_in_app":obj[5], "revenue_sum":obj[5] + float(obj[2]), "profit":float(obj[2]) + float(obj[5]) - float(obj[6])}
    #             })
    #         else:
    #             json_data_total[str(obj[0])]["cost"]  += float(obj[6])
    #             json_data_total[str(obj[0])]["revenue_ads"] += float(obj[2])
    #             json_data_total[str(obj[0])]["revenue_in_app"] += obj[5]
    #             json_data_total[str(obj[0])]["revenue_sum"] += obj[5] + float(obj[2]) 
    #             json_data_total[str(obj[0])]["profit"] += float(obj[2]) + float(obj[5]) - float(obj[6])
    #     data_sum.update({"data_by_date":list(json_data_total.values())})
    print("End calculate: " + str(datetime.now()))
    for x in data_sum:
        if isinstance(data_sum[x], float):
            data_sum[x] = round(data_sum[x],2)
    if 'data_by_date' in data_sum:
        for value in data_sum["data_by_date"]:
            for x in value:
                if isinstance(value[x], float):
                    value[x] = round(value[x],2)
    else:
        data_sum.update({'data_by_date':[{'date':datetime.today()}]})
    list_json_data = list(json_data.values())
    list_json_data.append(data_sum)
    for x in list_json_data:
        x["data_by_date"] = sorted(x["data_by_date"], key=lambda k: (k['date']))
    return {"data": list_json_data}

def check_data(data):
    if data is None:
        return ''
    return data
        