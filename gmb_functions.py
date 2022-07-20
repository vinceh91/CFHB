import datetime
import pandas as pd
import googleapiclient.discovery
from azure.storage.blob import BlobServiceClient
from io import BytesIO, StringIO
import numpy as np


def time_printer_ext():
    return datetime.datetime.now()


def rating_transco(ma_string):
    # Revu de la notation stars de google vers des entiers
    if ma_string == "FIVE":
        return 5
    elif ma_string == "FOUR":
        return 4
    elif ma_string == "THREE":
        return 3
    elif ma_string == "TWO":
        return 2
    elif ma_string == "ONE":
        return 1
    elif ma_string == "ZERO":
        return 0
    else:
        return np.nan


def extract_username(mon_dict):
    # formattage de colonne user
    try:
        return mon_dict["displayName"]
    except:
        return "not available"


def extract_location(ma_string):
    # formatage de colonne location
    try:
        return ma_string.split("reviews")[0][:-1]
    except:
        return "not available"


def to_datetime(ma_string):
    # formatage de colonnes date
    try:
        return datetime.datetime.strptime(ma_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        return datetime.datetime.strptime(ma_string, "%Y-%m-%dT%H:%M:%SZ")


def ggtranslation_delete(ma_str):
    # Formatage des traductions déjà fournies
    try:
        output = ma_str.split("(Original)")[1][1:]
        try:
            output = output.split("(Translated by Google)")[0][0:]
        except:
            output = output
    except:
        try:
            output = ma_str.split("(Translated by Google)")[0][0:]
        except:
            output = ma_str
    return output.replace("\r", "").replace("\n", "")


def delete_lines_return(ma_str):
    return ma_str.replace("\r", "").replace("\n", "")


def get_accounts_dataframe(service: googleapiclient.discovery.Resource) -> pd.DataFrame:
    """_summary_

    Args:
        service (googleapiclient.discovery.Resource): _description_

    Returns:
        pd.DataFrame: _description_
    """
    params = {}
    all_accounts = []
    page_token = None
    while True:
        if page_token:
            params["pageToken"] = page_token
        current_page = service.accounts().list(**params).execute()
        page_token = current_page.get("nextPageToken")
        all_accounts.extend(current_page["accounts"])
        if not page_token:
            break
    df_acc = pd.DataFrame(all_accounts)
    return df_acc


def get_locations(
    service: googleapiclient.discovery.Resource, accounts_list: dict
) -> pd.DataFrame:
    df_loc = pd.DataFrame()
    for _, acc in accounts_list:
        params = {"parent": acc, "readMask": "storeCode,name"}
        page_token = None
        while True:
            if page_token:
                params["pageToken"] = page_token
            current_page = service.accounts().locations().list(**params).execute()
            page_token = current_page.get("nextPageToken")
            current_locations = pd.DataFrame(current_page["locations"])
            current_locations["name"] = acc + "/" + current_locations["name"]
            df_loc = pd.concat([df_loc, current_locations])
            if not page_token:
                break
    return df_loc


def get_reviews(
    service: googleapiclient.discovery.Resource, df_loc: pd.DataFrame, date_ref: str
) -> pd.DataFrame:
    all_reviews = []
    for loc in df_loc["name"]:
        params = {"parent": loc}
        page_token = None
        while True:
            if page_token:
                params["pageToken"] = page_token
            current_page = (
                service.accounts().locations().reviews().list(**params).execute()
            )
            page_token = current_page.get("nextPageToken")
            all_reviews.extend(current_page["reviews"])
            if (not page_token) | (
                pd.DataFrame(current_page["reviews"]).createTime.min()[:10] < date_ref
            ):
                break
    df_reviews = pd.DataFrame(all_reviews)
    df_reviews = df_reviews.loc[df_reviews["createTime"] >= date_ref]
    df_reviews["comment"] = df_reviews["comment"].fillna("")  
    df_reviews["comment"] = df_reviews["comment"].apply(delete_lines_return)
    return df_reviews


def write_to_blob(
    blob_secret: str, container: str, blob: str, df: pd.DataFrame
) -> None:
    blobstock_service_client = BlobServiceClient.from_connection_string(blob_secret)
    blob_client = blobstock_service_client.get_blob_client(
        container=container, blob=blob
    )
    blob_client.delete_blob()
    with BytesIO(df.to_csv(sep="|", index=False).encode("utf-8")) as data:
        blob_client.upload_blob(data)
    return None


def preprocess_reviews(df: pd.DataFrame, df_stations: pd.DataFrame) -> pd.DataFrame:
    df["RESPONSERECOMMANDATION"] = df["starRating"].apply(rating_transco)
    df["USERNAME"] = "not GDPR compliant"
    df["location"] = df["name"].apply(extract_location)
    df["SURVEYDATE"] = df["createTime"].apply(to_datetime)
    df["REFUELDATE"] = np.nan
    df["USERID"] = "not available"
    df["SOURCE"] = "Google Reviews"
    df["comment"] = df["comment"].fillna("")
    df["comment"] = df["comment"].apply(ggtranslation_delete)
    

    df = df.merge(df_stations, left_on="location", right_on="name", how="left")

    df = df.rename(
        columns={
            "reviewId": "id",
            "storeCode": "STATIONID",
            "comment": "text",
            "Location Name": "STORENAME",
        }
    )
    df = df[
        [
            "id",
            "USERID",
            "SOURCE",
            "STATIONID",
            "SURVEYDATE",
            "REFUELDATE",
            "RESPONSERECOMMANDATION",
            "text",
            "USERNAME",
            "Country",
            "Country name",
            "REGION",
            "MANAGEMENT MODE",
            "STORENAME",
        ]
    ]
    return df


def read_from_blob(
    blob_secret: str, container: str, blob: str, sep: str = ";"
) -> pd.DataFrame:
    blobparam_service_client = BlobServiceClient.from_connection_string(blob_secret)
    blob_client = blobparam_service_client.get_blob_client(
        container=container, blob=blob
    )
    my_string = str(blob_client.download_blob().readall(), "latin-1")
    data = StringIO(my_string)
    df = pd.read_csv(data, sep=sep)
    return df
