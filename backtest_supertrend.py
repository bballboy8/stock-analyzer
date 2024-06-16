import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings

warnings.simplefilter("ignore")


def query_db(query, db_path="stock_data.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


df = query_db("select * from prices limit 7000")


def calculate_atr(df, length):
    df["H-L"] = df["High"] - df["Low"]
    df["H-PC"] = np.abs(df["High"] - df["Close"].shift(1))
    df["L-PC"] = np.abs(df["Low"] - df["Close"].shift(1))
    df["TR"] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)
    df["ATR"] = df["TR"].rolling(window=length).mean()
    return df


# Function to calculate Supertrend
def calculate_supertrend(df, multiplier, length):
    data = calculate_atr(df, length)
    data['BU'] = (data['High'] + data['Low']) / 2 + (multiplier * data['ATR'])
    data['BL'] = (data['High'] + data['Low']) / 2 - (multiplier * data['ATR'])

    # Initialize columns for Final Upper Band (FU), Final Lower Band (FL), and Supertrend
    data['FU'] = 0.0
    data['FL'] = 0.0
    data[f'Supertrend_{multiplier}_{length}'] = 0.0

    # Calculate Final Upper Band (FU) and Final Lower Band (FL)
    for i in range(1, len(data)):
        if data['Close'][i-1] <= data['FU'][i-1]:
            data['FU'][i] = min(data['BU'][i], data['FU'][i-1])
        else:
            data['FU'][i] = data['BU'][i]

        if data['Close'][i-1] >= data['FL'][i-1]:
            data['FL'][i] = max(data['BL'][i], data['FL'][i-1])
        else:
            data['FL'][i] = data['BL'][i]

    # Calculate Supertrend
    for i in range(len(data)):
        if data['Close'][i] > data['FU'][i]:
            data[f'Supertrend_{multiplier}_{length}'][i] = data['FL'][i]
        else:
            data[f'Supertrend_{multiplier}_{length}'][i] = data['FU'][i]
    return data


# Apply Supertrend calculation for each ticker and each set of parameters
tickers = df["ticker"].unique()
results = []
for ticker in tqdm(tickers):
    ticker_df = df[df["ticker"] == ticker].sort_values(by="Date")
    for factor, length in [(3, 12), (2, 11), (1, 10)]:
        result_df = calculate_supertrend(ticker_df, factor, length)
        results.append(result_df)

# Combine all results
final_df = pd.concat(results)
pd.set_option("display.max_rows", None)
print(final_df.tail()[["Date", "ticker", "Supertrend_1_10", "Supertrend_2_11", "Supertrend_3_12"]])
