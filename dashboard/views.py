import django
from django.shortcuts import render
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.template import loader
from .services import load_params, query
from .models import Game, GameUser, Country, Network
from django.contrib.auth.models import User
import functools
import os
import google.oauth2.credentials
import google_auth_oauthlib.flow
import urllib
import json
from django.shortcuts import redirect
from datetime import date, datetime, timedelta, time as datetime_time
from django.utils.datastructures import MultiValueDictKeyError
from google.auth.transport.requests import Request
import logging
from django.db.models import F, Value, CharField
from django.db.models.functions import Concat
from django.views.decorators.csrf import csrf_protect, csrf_exempt
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mkt_dashboard.settings");
django.setup()

logger = logging.getLogger('web')
# Create your views here.
def check_data(data):
    if data is None:
        return ''
    return data
@csrf_exempt
def home(request):
    """Renders the home page."""
    assert isinstance(request, HttpRequest)
    print(request.user.get_username())
    (game_name, platform, countries, ad_source, start_date, end_date, group_columns, partners, select_game, select_country, select_adsource, select_partner) = load_params(request)
    logger.info(str(datetime.now()) + " " + request.user.get_username() + " " + str(start_date) +":" +str(end_date))
    platform = check_data(platform)
    select_game = check_data(select_game)
    select_country = check_data(select_country)
    select_adsource = check_data(select_adsource)
    select_partner = check_data(select_partner)
    if request.user.get_username() in ["admin"]:
        list_game_model = Game.objects.values(game_id=F('_id'),game_name=Concat('name', Value(' ('), 'platform', Value(')')))
    else:
        list_game_model = GameUser.objects.get(user=request.user.get_username()).game.values(game_id=F('_id'),game_name=Concat('name', Value(' ('), 'platform', Value(')')))
    list_country_model = Country.objects.all()
    media_source = Network.objects.all()
    list_media_source = [i.name.replace(" ", "_") for i in media_source]
    list_media_source.sort()
    uas = []
    for game in list_game_model:
        for ua in GameUser.objects.filter(game=game['game_id']):
            if ua not in uas:
                uas.append(ua)
    list_partner = User.objects.filter(username__in=uas)
    template = loader.get_template('app/index.html')
    context = {
        'list_game': list_game_model,
        'list_media': list_media_source,
        'list_country': list_country_model,
        'list_partner': list_partner,
        'selected_game': select_game,
        'selected_country': select_country,
        'selected_adsource': select_adsource,
        'selected_platform': platform,
        'selected_company': select_partner,
        'count_app': str(len(list_game_model)),
        'count_country': str(len(list_country_model)),
        'count_ads': str(len(list_media_source)),
        'count_company': str(len(list_partner)),
        'start_date': str(start_date),
        'end_date': str(end_date),
        'break_value': '\'' + group_columns.replace(",", "\',\'") + '\'',
    }     
    template.render(context, request)
    return HttpResponse(template.render(context, request))

def test_admob(request):
    #admob.main()
    return JsonResponse({"status":200})

def provides_credentials(func):
    @functools.wraps(func)
    def wraps(request):
        #request.session = {}
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = "1"
        # If OAuth redirect response, get credentials
        SCOPES = ['https://www.googleapis.com/auth/admob.readonly','https://www.googleapis.com/auth/admob.report']
        CLIENT_SECRET_FILE = '/home/mkt-en/mkt_dashboard/conf/client_secret_web.json'
        flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
            CLIENT_SECRET_FILE, SCOPES,
            redirect_uri="https://data.abigames.com/oauth2")

        existing_state = request.GET.get('state', None)
        current_path = request.path
        if existing_state:
            secure_uri = request.build_absolute_uri()
            location_path = urllib.parse.urlparse(existing_state).path 
            flow.fetch_token(
                authorization_response=secure_uri,
                state=existing_state
            )
            request.session['credentials'] = flow.credentials.to_json()
            if location_path == current_path:
                return func(request, flow.credentials)
            # Head back to location stored in state when
            # it is different from the configured redirect uri
            return redirect(existing_state)

        # Otherwise, retrieve credential from request session.
        stored_credentials = request.session.get('credentials', None)
        print(stored_credentials)
        if not stored_credentials:
            # It's strongly recommended to encrypt state.
            # location is needed in state to remember it.
            location = request.build_absolute_uri() 
            # Commence OAuth dance.
            auth_url, _ = flow.authorization_url(state=location)
            return redirect(auth_url)
        # Hydrate stored credentials.
        credentials = google.oauth2.credentials.Credentials(**json.loads(stored_credentials))
        credentials.expiry = datetime.strptime( 
                credentials.expiry.rstrip("Z").split(".")[0], "%Y-%m-%dT%H:%M:%S" 
                ) 
        
        # If credential is expired, refresh it.
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        # Store JSON representation of credentials in session.
        request.session['credentials'] = credentials.to_json()
        with open("/home/mkt-en/mkt_dashboard/conf/admob.json", "w") as outfile:
            outfile.write(credentials.to_json())
            print("Success write file")
        return func(request, credentials=credentials)
    return wraps   

@provides_credentials
def test_AM(request, credentials):
    try:
        start_date = date.fromisoformat(request.GET['start_date'])
        request.session['start_date'] = request.GET['start_date']
    except MultiValueDictKeyError:
        start_date = date.fromisoformat(request.session.get('start_date','2022-03-01'))
    try:
        end_date = date.fromisoformat(request.GET['end_date'])
        request.session['end_date'] = request.GET['end_date']
    except MultiValueDictKeyError:
        end_date = date.fromisoformat(request.session.get('end_date','2022-03-31'))
    print(credentials.refresh_token, credentials.client_secret)
    # sql_text = 'Delete from app_data_revenue where source_id = \'Admob\' and date_update between \'' + str(start_date) + '\' and \'' + str(end_date) + '\''
    # DelData(sql_text)

    # source = SourceKey.objects.get(source_id = "Admob") 
    # update_database_admob(credentials, start_date, end_date, source)
    
    return HttpResponse(200)

def load_data(request):
    (game_name, platform, countries, ad_source, start_date, end_date, group_columns, partners, select_game, select_country, select_adsource, select_partner) = load_params(request)
    print(str(datetime.now()) + ": " + request.user.get_username() + " -- " + str(start_date) +":" +str(end_date))
    logger.info(request.user.get_username() + " -- " + str(start_date) +":" +str(end_date))
    return JsonResponse(query(request))

@csrf_exempt
def woocommerce_webhook(request):
    WEBHOOK_SECRET = 'aaaaaaaaaa'  # Replace 'aaaaaaaaaa' with your actual webhook secret
    if request.method == 'POST':
        # Verify the webhook secret
        try:
            payload = json.loads(request.body.decode('utf-8'))
            logger.info('Webhook data received: %s', payload)
            return JsonResponse({'status': 'success'}, status=200)
        except json.JSONDecodeError:
            logger.error(f'Invalid JSON data with body {request.body}')
            return JsonResponse({'error': 'Invalid JSON data'}, status=200)
    else:
        logger.info('Invalid request method')
        return JsonResponse({'error': 'Invalid request method'}, status=405)