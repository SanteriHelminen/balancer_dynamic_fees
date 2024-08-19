import os
import pandas as pd

def model(dbt, session):
    dbt.config(materialized="table", alias="klines_wbtc_usdt")

    # Get current working directory and set paths
    path = os.getcwd()
    klines_path = "models/cex_data/data/klines_wbtc_usdt"
    start_path_klines = os.path.join(path, klines_path)

    # Collect all CSV file paths
    klines_paths = [os.path.join(start_path_klines, file) for file in os.listdir(start_path_klines) if file.endswith(".csv")]
    klines_paths.sort()
    print("klines:", len(klines_paths))

    # Define the correct column names
    column_names = ["timestamp", "open", "high", "low", "close", "volume", "close_ts", "quote_volume", "trades_count", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"]

    # List to hold DataFrames
    dfs_klines1 = []

    # Read each CSV file, correct column names, and append to the respective list
    for filename in klines_paths:
        df = pd.read_csv(filename, header=None)  # Read without header
        df.columns = column_names  # Set correct column names
        dfs_klines1.append(df)
    
    # Concatenate all DataFrames into one
    df_klines1 = pd.concat(dfs_klines1, ignore_index=True)
    
    # Select and modify specific columns
    df_klines1['timestamp'] = df_klines1['timestamp'] / 1000
    df_klines1['close_ts'] = df_klines1['close_ts'] / 1000
    df_klines1 = df_klines1.sort_values(by='timestamp')
    
    return df_klines1
