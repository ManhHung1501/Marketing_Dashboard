from dashboard.models import Exchange
import requests

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