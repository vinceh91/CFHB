from gmb_functions import preprocess_reviews
from azure.storage.blob import BlobServiceClient
import config
import pandas as pd
import datetime
from gmb_functions import read_from_blob

container_stock = 'data'
container_param = 'customervoice-param'
date = datetime.datetime.now()
date = str(date.day + date.month*100 + date.year*10000)
fl_fbacks = "feedbacks/fbacks_"+ date +".csv"
fl_original = "original/data_"+ date +".csv"
fl_luis = "luis/luis_"+ date +".csv"
fl_phrases = "keyphrases/phrases_"+date+".csv"

blob_stock = config.BLOB_STOCK_KEY
blob_param = config.BLOB_PARAM_STRING

df_reviews = read_from_blob(blob_stock, container_stock, fl_original)
df_stations = read_from_blob(blob_param, container_param, "station_list.csv" )
df_reviews_preprocessed = preprocess_reviews(df_reviews, df_stations)

### Text Translation
analysis_language =  "en" #"en-us"
region_text_translation = 'westeurope'
host_text_translation = 'https://api.cognitive.microsofttranslator.com'
path_text_translation = '/translate?api-version=3.0'
params = "&to=" + analysis_language
url_text_translation = host_text_translation + path_text_translation + params

### Text Analysis
region_text_analysis = 'westeurope'
host_text_analysis = 'https://'+ region_text_analysis +'.api.cognitive.microsoft.com/text/analytics/v2.0'


df_feedbacks = TheVoice_multi_lines(df_rev,
                    Key_Text_Translation,
                    Region_Text_Translation,
                    Url_Text_Translation,
                    Key_Text_Analysis,
                    Region_Text_Analysis,
                    Host_Text_Analysis,
                    Key_LUIS, luis_apps,
                    connect_param)
print("reviews processed - " + str(datetime.datetime.now()))
df_feedbacks["id_unique"]= df_feedbacks.id 
df_feedbacks.suggested = df_feedbacks.suggested.fillna('')
df_feedbacks['suggested'] = df_feedbacks.suggested.apply(linesreturn_delete)