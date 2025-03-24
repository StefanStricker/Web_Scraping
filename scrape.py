import requests
import pandas as pd
from api_tokens import green_api, tank_api, alpha_api
from datetime import datetime
import logging

logging.basicConfig(filename="log_file", level=logging.DEBUG, 
                    format="%(asctime)s:%(levelname)s:%(message)s")



def get_api(url, headers, querystring):
    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        logging.debug(data = response.json())

    else: logging.error(print(response.status_code))


#https://gruenstromindex.de/

green_url = "https://api.corrently.io/v2.0/gsi/prediction"

querystring = {"zip":"10178",
               "token":green_api
               }

headers = {
    "Accept": "application/json"
    }
   
response = requests.get(green_url, headers=headers, params=querystring)

data = response.json()

df = pd.DataFrame(data["forecast"])

df.drop(columns=["eevalue", "ensolar", "timeStamp", "iat", "timeframe", "sci", "co2_avg", "co2_g_standard", "co2_g_oekostrom", "ewind", "esolar", "enwind", "zip", "signature"], inplace=True)

df['epochtime'] = pd.to_datetime(df['epochtime'], unit='s')
df.rename(columns={"epochtime": "time"},  inplace=True)

df.to_csv("grünstrom.csv", index = False)




#https://www.tankerkoenig.de/
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

tank_response = requests.get(tank_url, headers = tank_headers, params=tank_querystring)

tank_data = tank_response.json()

df_tank = pd.json_normalize(tank_data, record_path="stations")

df_tank.drop(columns= ["id", "brand", "lat", "lng"], inplace=True)

#print(df_tank.head(25))

df_tank.to_csv("gas_price.csv", index=False)




#https://www.alphavantage.co/
alpha_url = "https://www.alphavantage.co/query"

alpha_params = {
    "apikey":alpha_api,
    "function":"WTI",
    "interval":"daily"
}

alpha_response = requests.get(alpha_url, params=alpha_params)

alpha_data = alpha_response.json()

df_alpha = pd.DataFrame(alpha_data["data"])

print(df_alpha.head())

df_alpha.to_csv("brent.csv", index=False)

