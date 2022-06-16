import pickle, os, datetime
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient
from io import BytesIO, StringIO
from IPython.display import display
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import googleapiclient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

### LUIS
luis_apps = {'fr' : '0a5be55e-0c32-482f-abcc-287cb9952f40',
             'en' : '11fe55ad-3644-475e-a838-a49ed6a94ded',
             'nl' : 'f176f20b-a4fa-470c-af05-ea4e72343412' }

## Param spécifiques
account_list = ['accounts/102280495497198834033', 'accounts/102543083042626102334', 'accounts/116116273817792355878']  # BE + NL + LU
#account_list = ['accounts/108458503997377848869'] #KE

nb_jours_reprise = 1
get_accounts = True
get_locations = True
get_reviews = True
cfh_scope = "cfhb"
analysis_language =  "en" #"en-us"

## Param génériques
### GMB
SCOPES = ['https://www.googleapis.com/auth/business.manage']
discovery_uri = "https://developers.google.com/my-business/samples/mybusiness_google_rest_v4p9.json"
# discovery_uri = "https://mybusiness.googleapis.com/$discovery/rest?version=v4"
discovery_uri_info = "https://mybusinessbusinessinformation.googleapis.com/$discovery/rest?version=v1"
discovery_uri_manag = "https://mybusinessaccountmanagement.googleapis.com/$discovery/rest?version=v1"

gmb_token_file = 'token_cfh.pickle'
### Process et init varialbes
__next_step__ = True
creds = None
date = datetime.datetime.now()
date = str(date.day + date.month*100 + date.year*10000)
date_ref = str(datetime.datetime.now()-datetime.timedelta(days=nb_jours_reprise))[:10]
### blob & file systems
container_param = 'customervoice-param'
container_stock = 'data'
container_gmb_ref = 'gmb-data-ref'
fl_fbacks = "feedbacks/fbacks_"+date+".csv"
fl_original = "original/data_"+date+".csv"
fl_luis = "luis/luis_"+date+".csv"
fl_phrases = "keyphrases/phrases_"+date+".csv"

########################################################################################
#Getting secrets

#Storage accounts
# blob_param = dbutils.secrets.get(scope = cfh_scope, key = "blob-param-string")
# blob_stock = dbutils.secrets.get(scope = cfh_scope, key = "blob-stock-string")
keyVaultName = "azkvpcfhb02" 
KVUri = f"https://{keyVaultName}.vault.azure.net"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)
secretName = "blob-param-string"
retrieved_secret = client.get_secret(secretName)
# #Services cognitifs
# Key_Text_Translation = dbutils.secrets.get(scope = cfh_scope, key = "tt-key")
# Key_Text_Analysis = dbutils.secrets.get(scope = cfh_scope, key = "ta-key")
# Key_LUIS = dbutils.secrets.get(scope = cfh_scope, key = "lu-key")
# #SQL DB

# # mise en forme
# connect_param = {'string' : blob_param,
#                  'container' : container_param }