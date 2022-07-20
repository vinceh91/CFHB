from gmb_functions import preprocess_reviews, read_from_blob, delete_lines_return, write_to_blob
from azure.storage.blob import BlobServiceClient
import pandas as pd
import datetime
from ai_functions import the_voice_multi_lines
from config import *
from pandas.io.json import json_normalize

date = datetime.datetime.now()
date = str(date.day + date.month * 100 + date.year * 10000)
fl_fbacks = "feedbacks/fbacks_" + date + ".csv"
fl_original = "original/data_" + date + ".csv"
fl_luis = "luis/luis_" + date + ".csv"
fl_phrases = "keyphrases/phrases_" + date + ".csv"
params = "&to=" + ANALYSIS_LANGUAGE
url_text_translation = HOST_TEXT_TRANSLATION + PATH_TEXT_TRANSLATION + params
connect_param = {"string": BLOB_PARAM_STRING, "container": CONTAINER_PARAM}
host_text_analysis = (
    "https://"
    + REGION_TEXT_ANALYSIS
    + ".api.cognitive.microsoft.com/text/analytics/v2.0"
)

df_reviews = read_from_blob(BLOB_STOCK_STRING, CONTAINER_STOCK, fl_original, sep="|")
df_stations = read_from_blob(BLOB_PARAM_STRING, CONTAINER_PARAM, "station_list.csv")
df_locations = read_from_blob(BLOB_STOCK_STRING, CONTAINER_GMB_REF, "station_raw.csv", sep="|")
df_stations = df_stations[["Store ID", "Country", "Country name", "REGION", "MANAGEMENT MODE", "Location Name"]]
df_stations = df_stations.merge(df_locations[["storeCode","name"]],
                                        left_on="Store ID",
                                        right_on="storeCode",
                                        how="right")
df_reviews_preprocessed = preprocess_reviews(df_reviews, df_stations)

df_feedbacks = the_voice_multi_lines(
    df_reviews_preprocessed,
    TT_KEY,
    REGION_TEXT_TRANSLATION,
    url_text_translation,
    TA_KEY,
    REGION_TEXT_ANALYSIS,
    host_text_analysis,
    LU_KEY,
    LUIS_APPS,
    connect_param,
)

df_feedbacks["id_unique"] = df_feedbacks["id"]
df_feedbacks["suggested"] = df_feedbacks["suggested"].fillna("")
df_feedbacks["suggested"] = df_feedbacks["suggested"].apply(delete_lines_return)

#code changé donc à checker attention
key_phrases_df = df_feedbacks[~df_feedbacks["key_phrases"].isin(["nan", "NaN",""," "])]["key_phrases"]
key_phrases_df = key_phrases_df.reset_index(drop=True)

luis_df = df_feedbacks[~df_feedbacks["luis"].isin(["nan", "NaN",""," "])]["luis"]
luis_df = json_normalize(luis_df)
luis_df = luis_df.reset_index(drop=True)

write_to_blob(BLOB_STOCK_KEY, CONTAINER_STOCK, fl_fbacks, df_feedbacks)
write_to_blob(BLOB_STOCK_KEY, CONTAINER_STOCK, fl_luis, luis_df)
write_to_blob(BLOB_STOCK_KEY, CONTAINER_STOCK, fl_phrases, key_phrases_df)
