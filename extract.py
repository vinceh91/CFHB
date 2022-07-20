import pickle
import datetime
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
from params import params_dict
import logging
import sys
import os

date = datetime.datetime.now()
date = str(date.day + date.month*100 + date.year*10000)
date_ref = str(datetime.datetime.now() -
               datetime.timedelta(days=params_dict["nb_days_before"]))[:10]
fl_fbacks = "feedbacks/fbacks_" + date + ".csv"
fl_original = "original/data_" + date + ".csv"
fl_luis = "luis/luis_" + date + ".csv"
fl_phrases = "keyphrases/phrases_" + date + ".csv"

blobparam_service_client = BlobServiceClient.from_connection_string(
    BLOB_PARAM_STRING)
blob_client = blobparam_service_client.get_blob_client(container=CONTAINER_PARAM,
                                                       blob=GMB_TOKEN_FILE)
creds = pickle.loads(blob_client.download_blob().readall())
creds.refresh(Request())

accounts_service = build('mybusinessaccountmanagement',
                         'v1', credentials=creds)
df_acc = get_accounts_dataframe(accounts_service)

locations_service = build(
    'mybusinessbusinessinformation', 'v1', credentials=creds)
df_locations = get_locations(locations_service, ACCOUNTS_LIST)

blob_client = blobparam_service_client.get_blob_client(container='customervoice-param',
                                                       blob="station_list.csv")
my_string = str(blob_client.download_blob().readall(), 'latin-1')
data = StringIO(my_string)
df_stations = pd.read_csv(data, sep=";")
df_stations = df_stations[['Store ID', 'Country',
                           'Country name', 'REGION', 'MANAGEMENT MODE', 'Location Name']]
df_stations = df_stations.merge(df_locations[['storeCode', 'name']],
                                left_on='Store ID',
                                right_on='storeCode',
                                how='right')

reviews_service = build('mybusiness', 'v4', credentials=creds,
                        discoveryServiceUrl=DISCOVERY_URI, static_discovery=False)
df_reviews = get_reviews(reviews_service, df_locations, date_ref)

# Write to blob
write_to_blob(BLOB_STOCK_KEY, CONTAINER_GMB_REF,
              "location_accounts.csv", df_acc)
write_to_blob(BLOB_STOCK_KEY, CONTAINER_GMB_REF,
              "station_raw.csv", df_locations)
write_to_blob(BLOB_STOCK_KEY, CONTAINER_GMB_REF,
              "station_enrich.csv", df_stations)
write_to_blob(BLOB_STOCK_KEY, CONTAINER_STOCK, fl_original, df_reviews)

df_reviews_preprocessed = preprocess_reviews(df_reviews, df_stations)
