import pickle, datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO
from IPython.display import display
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import googleapiclient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, VisualStudioCodeCredential
from config import *
from gmb_functions import get_locations, preprocess_reviews, write_to_blob, get_accounts_dataframe, get_reviews
import requests

import logging
import sys

# ...
import os
# os.environ["HTTP_PROXY"] = "http://10.10.1.10:1180"

# Alternate URL and variable forms:
# os.environ["HTTP_PROXY"] = "http://username:password@10.10.1.10:1180"
# os.environ["HTTPS_PROXY"] = "http://10.10.1.10:1180"
# os.environ["HTTPS_PROXY"] = "http://username:password@10.10.1.10:1180"
# Acquire the logger for a library (azure.mgmt.resource in this example)
# logger = logging.getLogger('azure')

# Set the desired logging level
# logger.setLevel(logging.DEBUG)

# handler = logging.StreamHandler(stream=sys.stdout)
# logger.addHandler(handler)

## Param spécifiques
accounts_list = ['accounts/102280495497198834033', 'accounts/102543083042626102334', 'accounts/116116273817792355878']  # BE + NL + LU
#account_list = ['accounts/108458503997377848869'] #KE

nb_jours_reprise = 1
get_accounts = True
# get_locations = True
get_reviews = True
cfh_scope = "cfhb"

## Param génériques
### GMB
# SCOPES = ['https://www.googleapis.com/auth/business.manage']
discovery_uri = "https://developers.google.com/my-business/samples/mybusiness_google_rest_v4p9.json"
# discovery_uri = "https://mybusiness.googleapis.com/$discovery/rest?version=v4"
discovery_uri_info = "https://mybusinessbusinessinformation.googleapis.com/$discovery/rest?version=v1"
discovery_uri_manag = "https://mybusinessaccountmanagement.googleapis.com/$discovery/rest?version=v1"

gmb_token_file = 'token_cfh.pickle'
### Process et init variables
creds = None
date = datetime.datetime.now()
date = str(date.day + date.month*100 + date.year*10000)
date_ref = str(datetime.datetime.now() - datetime.timedelta(days=nb_jours_reprise))[:10]
fl_fbacks = "feedbacks/fbacks_"+ date +".csv"
fl_original = "original/data_"+ date +".csv"
fl_luis = "luis/luis_"+ date +".csv"
fl_phrases = "keyphrases/phrases_"+date+".csv"

########################################################################################
#Getting secrets

#Storage accounts

# keyVaultName = "azkvpcfhb02" 
# KVUri = f"https://{keyVaultName}.vault.azure.net"
# credential = DefaultAzureCredential(ogging_enable=True)
# # credential = VisualStudioCodeCredential(logging_enable=True)
# client = SecretClient(vault_url=KVUri, credential=credential, logging_enable=True, proxies={ "https": "https://AJ0545269@admin.hubtotal.net:NDjokovic91!@10.10.1.10:1180"})
# secretName = "blob-param-string"
# retrieved_secret = client.get_secret(secretName).value


blobparam_service_client = BlobServiceClient.from_connection_string(BLOB_PARAM_STRING)
blob_client = blobparam_service_client.get_blob_client(container=CONTAINER_PARAM,
                                                                  blob=gmb_token_file)        
creds = pickle.loads(blob_client.download_blob().readall())
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("Credentials Refresh : OK")
    elif creds.valid:
        creds.refresh(Request())
        print("Credentials Refresh : OK")

accounts_service = build('mybusinessaccountmanagement', 'v1', credentials=creds)
df_acc = get_accounts_dataframe(accounts_service)

locations_service = build('mybusinessbusinessinformation', 'v1', credentials=creds)
df_locations = get_locations(locations_service, accounts_list)

blob_client = blobparam_service_client.get_blob_client(container='customervoice-param',
                                                  blob="station_list.csv")
my_string = str(blob_client.download_blob().readall() ,'latin-1')
data = StringIO(my_string) 
df_stations = pd.read_csv(data, sep=";")
df_stations = df_stations[['Store ID', 'Country', 'Country name', 'REGION', 'MANAGEMENT MODE', 'Location Name']]
df_stations = df_stations.merge(df_locations[['storeCode','name']],
                                        left_on='Store ID',
                                        right_on='storeCode',
                                        how='right')

reviews_service = build('mybusiness', 'v4', credentials=creds, discoveryServiceUrl = discovery_uri, static_discovery=False)
df_reviews = get_reviews(reviews_service, df_locations, date_ref)

#### Write to blob
write_to_blob(BLOB_STOCK_KEY, CONTAINER_GMB_REF, "location_accounts.csv", df_acc)
write_to_blob(BLOB_STOCK_KEY, CONTAINER_GMB_REF, "station_raw.csv", df_locations)
write_to_blob(BLOB_STOCK_KEY, CONTAINER_GMB_REF, "station_enrich.csv", df_stations)
write_to_blob(BLOB_STOCK_KEY, CONTAINER_STOCK, fl_original, df_reviews)

df_reviews_preprocessed = preprocess_reviews(df_reviews, df_stations)

#### write to blob

