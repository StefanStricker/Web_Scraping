import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

store = pd.HDFStore("data.h5")

# Plotting Green Strom GSI
green_keys = [key for key in store.keys() if key.startswith("/green")]

records = []

for key in green_keys:
    df = store.get(key)

    date = key.split("/")[-1]

    if "gsi" in df.columns:
        gsi_avg = df["gsi"].mean() 
        records.append({"date": date, "gsi": gsi_avg})


df_gsi = pd.DataFrame(records)
df_gsi["date"] = pd.to_datetime(df_gsi["date"])
df_gsi.sort_values("date", inplace=True)

plt.figure(figsize=(10, 5))
plt.plot(df_gsi["date"], df_gsi["gsi"])
plt.title("Daily Average GSI (Green Strom Index)")
plt.xlabel("Date")
plt.ylabel("GSI")
plt.grid(True)
plt.tight_layout()
plt.savefig("gsi.png")


#Plotting Brent Oil Price
oil_keys = sorted(
    [key for key in store.keys() if key.startswith("/oil/")],
    key=lambda k: k.split("/")[-1]
)

latest_key = oil_keys[-1]

df_latest_oil = store.get(latest_key)
df_latest_oil = df_latest_oil.head(6).iloc[::-1]
df_latest_oil["value"] = pd.to_numeric(df_latest_oil["value"])

plt.figure(figsize=(10, 5))
plt.plot(df_latest_oil["date"], df_latest_oil["value"])
plt.title("Daily Oil Value")
plt.xlabel("Date")
plt.ylabel("Dollars per barrel")
plt.grid(True)
plt.tight_layout()
plt.savefig("oil.png")

#Plotting Gas Price
fuel_keys = [key for key in store.keys() if key.startswith("/fuel")]

records = []

for key in fuel_keys:
    df = store.get(key)

    date = key.split("/")[-1]
    df = df[df["dist"] < 5]

    if "diesel" in df.columns:
        diesel_avg = df["diesel"].mean() 
        records.append({"date": date, "diesel": diesel_avg})

df_fuel = pd.DataFrame(records)
df_fuel["date"] = pd.to_datetime(df_fuel["date"])
df_fuel.sort_values("date", inplace=True)

store.close()

plt.figure(figsize=(10, 5))
plt.plot(df_fuel["date"], df_fuel["diesel"])
plt.title("Average Diesel Price in 5km radius")
plt.xlabel("Date")
plt.ylabel("Diesel Price in €")
plt.grid(True)
plt.tight_layout()
plt.savefig("diesel.png")
