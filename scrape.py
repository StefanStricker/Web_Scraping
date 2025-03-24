import requests
import pandas as pd
from api_tokens import green_api, tank_api, alpha_api
from datetime import datetime
import time
import logging
import h5py



#setup loggin
logging.basicConfig(filename="log_file", level=logging.DEBUG, 
                    format="%(asctime)s:%(levelname)s:%(message)s")

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


#function to call API
def get_api(url, headers, querystring):
    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        data = response.json()
        logging.debug("API call successful")
        return data

    else: 
        logging.error(f"API request failed. Status code: {response.status_code}")


#get data from https://gruenstromindex.de/
green_url = "https://api.corrently.io/v2.0/gsi/prediction"

querystring = {"zip":"10178",
               "token":green_api
               }

headers = {
    "Accept": "application/json"
    }
   
data = get_api(url=green_url, headers=headers, querystring=querystring)

df = pd.DataFrame(data["forecast"])

df.drop(columns=["eevalue", "ensolar", "timeStamp", "iat", "timeframe", "sci", "co2_avg", "co2_g_standard", "co2_g_oekostrom", "ewind", "esolar", "enwind", "zip", "signature"], inplace=True)

df['epochtime'] = pd.to_datetime(df['epochtime'], unit='s')
df.rename(columns={"epochtime": "time"},  inplace=True)

df.to_csv("grünstrom.csv", index = False)




#get data from https://www.tankerkoenig.de/
tank_url = "https://creativecommons.tankerkoenig.de/json/list.php"

tank_headers = {
    "Accept": "application/json",
    "charset": "utf-8"
    }

tank_querystring = {
    "apikey":tank_api,
    "lat":"52.5",
    "lng":"13.4",
    "rad":"10",
    "type": "all",
    "sort":"dist"
}

tank_data = get_api(tank_url, tank_headers, tank_querystring)

df_tank = pd.json_normalize(tank_data, record_path="stations")

df_tank.drop(columns= ["id", "brand", "lat", "lng"], inplace=True)

df_tank.to_csv("gas_price.csv", index=False)




#get data from https://www.alphavantage.co/
alpha_url = "https://www.alphavantage.co/query"

alpha_params = {
    "apikey":alpha_api,
    "function":"WTI",
    "interval":"daily"
}

alpha_data = get_api(url=alpha_url, querystring=alpha_params, headers=None)

df_alpha = pd.DataFrame(alpha_data["data"])

df_alpha.to_csv("brent.csv", index=False)


#create csv file with webpages info
urls = [green_url, tank_url, alpha_url]
urls_df = pd.DataFrame({"url": urls})
urls_df.to_csv("webpages.csv", index=False)

