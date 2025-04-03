import pandas as pd
import matplotlib.pyplot as plt

store = pd.HDFStore("data.h5")

#print(store.keys())

green_keys = [key for key in store.keys() if key.startswith("/green")]
oil_keys = [key for key in store.keys() if key.startswith("/oil")]
fuel_keys = [key for key in store.keys() if key.startswith("/fuel")]


# Prepare list to hold values
records = []

# Loop over green keys
for key in green_keys:
    df = store.get(key)

    # Get the date from the key
    date = key.split("/")[-1]

    # Optional: inspect available columns
    # print(df.columns)

    # Extract a single numeric value (e.g., average of 'gsi' column)
    if "gsi" in df.columns:
        gsi_avg = df["gsi"].mean()  # or use df["gsi"].iloc[0] if there's only 1 row
        records.append({"date": date, "gsi": gsi_avg})

store.close()

# Convert to DataFrame
df_gsi = pd.DataFrame(records)
df_gsi["date"] = pd.to_datetime(df_gsi["date"])
df_gsi.sort_values("date", inplace=True)

# Plot
plt.figure(figsize=(10, 5))
plt.plot(df_gsi["date"], df_gsi["gsi"], marker="o", linestyle="-")
plt.title("Daily Average GSI (Green Strom Index)")
plt.xlabel("Date")
plt.ylabel("GSI")
plt.grid(True)
plt.tight_layout()
plt.savefig("gsi.png")
