import hashlib
import time
import requests
from network_config import read_xml
import hashlib
import os
import re
import socket
import sys
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

class PangleMediaUtil:
    secure_key,role_id, access_token = read_xml('Pangle') 
    user_id = role_id
    

    version = "2.0"
    sign_type_md5 = "MD5"
    KEY_USER_ID = "user_id"
    KEY_ROLE_ID = "role_id"
    KEY_VERSION = "version"
    KEY_SIGN    = "sign"
    KEY_SIGN_TYPE = "sign_type"
    PANGLE_HOST = "https://open-api.pangleglobal.com"

    @classmethod
    def sign_gen(self, params):
        """Fetches sign .
        Args:
        params: a dict need to sign
        secure_key: string

        Returns:
        A dict. For example:

        {'url': 'a=1&sign_type=MD5&t=2&z=a&sign=7ff19ec1961d8c2b7c7b3845d974d22e',
        'sign': '7ff19ec1961d8c2b7c7b3845d974d22e'}
        """
        result = {
            "sign": "",
            "url": "",
        }
        try:
            if not isinstance(params, dict):
                print("invalid params: ", params)
                return result

            if self.user_id != "":
                params[self.KEY_USER_ID] = self.user_id

            if self.role_id != "":
                params[self.KEY_ROLE_ID] = self.role_id

            params[self.KEY_VERSION] = self.version
            params[self.KEY_SIGN_TYPE] = self.sign_type_md5

            param_orders = sorted(params.items(), key=lambda x: x[0], reverse=False)
            raw_str = ""
            for k, v in param_orders:
                if v == "":
                    continue
                raw_str += (str(k) + "=" + str(v) + "&")
                if len(raw_str) == 0:
                    return ""
                sign_str = raw_str[0:-1] + self.secure_key

            sign = hashlib.md5(sign_str.encode()).hexdigest()
            result[self.KEY_SIGN] = sign
            result["url"] = raw_str + "sign=" + sign
            return result
        except Exception as err:
            print("invalid Exception", err)
            return result

    @classmethod
    def get_signed_url(self, params):
        return self.sign_gen(params).get("url", "")

    @classmethod
    def get_media_rt_income(self, params):
        result = self.get_signed_url(params)
        if result == "":
            return ""
        return self.PANGLE_HOST + "/union_pangle/open/api/rt/income?" + result

    @classmethod
    def get_sig(self, params):
        result = self.get_signed_url(params)
        if result == "":
            return ""
        else:
            key_value_pairs = result.split('&')

            # Iterate through the key-value pairs to find 'r_id'
            for pair in key_value_pairs:
                key, value = pair.split('=')
                if key == 'sign':
                    sign = value
                    break  # Stop searching once 'r_id' is foun
        return sign

def sign_mintegral(secret):
    time_req = str(int(time.time()))
    md5_time = hashlib.md5(time_req.encode()).hexdigest()
    combine_str = secret + md5_time
    sign = hashlib.md5(combine_str.encode()).hexdigest()

    return time_req, sign

def ironsource_key(secret_key, refreshToken):
    url = "https://platform.ironsrc.com/partners/publisher/auth"
    headers = {
        "secretkey" : secret_key,
        "refreshToken" : refreshToken
    }
    
    response = requests.get(url , headers=headers)
    
    return response.text

def inmobi_session(userName, secretKey):
    headers = {
        'userName': userName,
        'secretKey': secretKey,
    }
    generate_url = 'https://api.inmobi.com/v1.0/generatesession/generate'

    # Make the GET request
    response = requests.get(generate_url, headers=headers)
    data = response.json()["respList"][0]
    sessionId = data["sessionId"]
    accountId = data["accountId"]
    
    return sessionId, accountId
