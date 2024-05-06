import json
import requests
from requests.structures import CaseInsensitiveDict

def request_app(information):
    auth_key = information
    auth_url = "https://auth-api.vungle.com/login/"
    headers_auth = CaseInsensitiveDict()
    headers_auth["accept"] = "application/json"
    headers_auth["vungle-source"] = "api"
    headers_auth["vungle-version"] = "1"
    headers_auth["Content-Type"] = "application/json"
    res_auth = requests.post(auth_url, headers=headers_auth, data=auth_key)
    if res_auth.status_code != 200:
        if res_auth.status_code == 404:
            print('There is a problem with the request URL Auth Vungle. Make sure that it is correct')
        else:
            print('There was a problem retrieving data Auth Vungle: ', res_auth.text, res_auth.status_code)
        return res_auth.text
    auth_json = res_auth.json()
    app_url = "https://publisher-api.vungle.com/api/v1/applications/"
    headers_app = CaseInsensitiveDict()
    headers_app["accept"] = "application/json"
    headers_app["vungle-source"] = "api"
    headers_app["vungle-version"] = "1"
    headers_app["Content-Type"] = "application/json"
    headers_app["Authorization"] = "Bearer " + str(auth_json["token"])
    res_app = requests.get(app_url, headers=headers_app)
    if res_app.status_code != 200:
        if res_app.status_code == 404:
            print('There is a problem with the request URL Auth Vungle. Make sure that it is correct')
        else:
            print('There was a problem retrieving data Auth Vungle: ', res_app.text, res_app.status_code)
        return res_app.text
    json_object = json.dumps(res_app.json())
    with open("/home/mkt-en/mkt_dashboard/conf/app_vungle.json", "w") as outfile:
        outfile.write(json_object)
        print("Success write file")