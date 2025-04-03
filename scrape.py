import requests
import pandas as pd
from api_tokens import green_api, tank_api, alpha_api
from datetime import datetime
import os
import logging
from pandas import HDFStore


#setup loggin
logging.basicConfig(filename="log_file", level=logging.DEBUG, 
                    format="%(asctime)s:%(levelname)s:%(message)s")

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


##API Configurations Dictionary

apis = {
    #https://gruenstromindex.de/
    "green": {
        "url": "https://api.corrently.io/v2.0/gsi/prediction",
        "headers": {"Accept": "application/json"},
        "params": {"zip": "10178", "token": green_api}
    },
    #https://www.tankerkoenig.de/
    "fuel": {
        "url": "https://creativecommons.tankerkoenig.de/json/list.php",
        "headers": {"Accept": "application/json", "charset": "utf-8"},
        "params": {"apikey": tank_api, "lat": "52.5", "lng": "13.4", "rad": "10", "type": "all", "sort": "dist"}
    },
    #https://www.alphavantage.co/
    "oil": {
        "url": "https://www.alphavantage.co/query",
        "headers": None,
        "params": {"apikey": alpha_api, "function": "WTI", "interval": "daily"}
    }
}

#request API function
def get_api(url, headers, params):
    try:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            logging.debug("API call successful")
            return data

        else: 
            logging.error(f"API request failed. Status code: {response.status_code}")
            return None 
        
    except Exception as e:
        logging.error(f"API request failed. Error: {e}")
        return None        

#Function to process JSON to DataFrame
def json_to_dataframe(name, json_data):
    if not json_data:
        return None

    if name == "green" and "forecast" in json_data:
        df = pd.DataFrame(json_data["forecast"])

    elif name == "fuel" and "stations" in json_data:
        df = pd.json_normalize(json_data, record_path="stations")

    elif name == "oil" and "data" in json_data:
        df = pd.DataFrame(json_data["data"])

    else:
        df = pd.json_normalize(json_data)

    if "epochtime" in df.columns:
        df["epochtime"] = pd.to_datetime(df["epochtime"], unit='s')
        df.rename(columns={"epochtime": "time"}, inplace=True)

    return df

# Save all DataFrames to HDF5 file
def save_all_to_hdf5(dataframes_dict, filename="data.h5"):
    today = datetime.now().strftime("%Y-%m-%d")
    with HDFStore(filename) as store:
        for name, df in dataframes_dict.items():
            if df is not None and not df.empty:
                store.put(f"/{name}/{today}", df)
                logging.info(f"Saved {name} data to HDF5")
            else:
                logging.warning(f"No data to save for {name}")



#saves api calls to csv
def api_calls_csv(apis, log="webpages.csv"):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    new_log = pd.DataFrame({
        "name": list(apis.keys()),
        "webpageurl": [cfg["url"] for cfg in apis.values()],
        now: ["called"] * len(apis)
    })

    if os.path.exists(log):
        existing_log = pd.read_csv(log)

        merged = pd.merge(existing_log, new_log, on=["name", "webpageurl"], how="outer")
        merged.to_csv(log, index=False)
    else:
        new_log.to_csv(log, index=False)

    logging.info(f"Updated API call log at {log}")




def main():
    json_data = {}
    dfs = {}

    for name, cfg in apis.items():
        json_result = get_api(cfg["url"], cfg["headers"], cfg["params"])
        json_data[name] = json_result
        df = json_to_dataframe(name, json_result)
        dfs[name] = df

    save_all_to_hdf5(dfs)
    api_calls_csv(apis)


if __name__ == "__main__":
    main()

    