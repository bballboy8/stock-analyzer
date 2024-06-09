import sqlite3
import pandas as pd
import numpy as np


def query_db(query, db_path="stock_data.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


df = query_db("select * from prices limit 100")


def calculate_atr(df, length):
    df["H-L"] = df["High"] - df["Low"]
    df["H-PC"] = np.abs(df["High"] - df["Close"].shift(1))
    df["L-PC"] = np.abs(df["Low"] - df["Close"].shift(1))
    df["TR"] = df[["H-L", "H-PC", "L-PC"]].max(axis=1)
    df["ATR"] = df["TR"].rolling(window=length).mean()
    return df


# Function to calculate Supertrend
def calculate_supertrend(df, factor, length):
    df = calculate_atr(df, length)
    df["Upper Band"] = ((df["High"] + df["Low"]) / 2) + (factor * df["ATR"])
    df["Lower Band"] = ((df["High"] + df["Low"]) / 2) - (factor * df["ATR"])
    df["In Uptrend"] = True

    for current in range(1, len(df.index)):
        previous = current - 1

        if df["Close"][current] > df["Upper Band"][previous]:
            df["In Uptrend"][current] = True
        elif df["Close"][current] < df["Lower Band"][previous]:
            df["In Uptrend"][current] = False
        else:
            df["In Uptrend"][current] = df["In Uptrend"][previous]
            if (
                df["In Uptrend"][current]
                and df["Lower Band"][current] < df["Lower Band"][previous]
            ):
                df["Lower Band"][current] = df["Lower Band"][previous]
            if (
                not df["In Uptrend"][current]
                and df["Upper Band"][current] > df["Upper Band"][previous]
            ):
                df["Upper Band"][current] = df["Upper Band"][previous]

    df["Supertrend"] = np.where(df["In Uptrend"], df["Lower Band"], df["Upper Band"])
    return df


# Apply Supertrend calculation for each ticker and each set of parameters
tickers = df["ticker"].unique()
results = []
for ticker in tickers:
    ticker_df = df[df["ticker"] == ticker].copy()
    for factor, length in [(3, 12), (2, 11), (1, 10)]:
        result_df = calculate_supertrend(ticker_df, factor, length)
        result_df["Factor"] = factor
        result_df["Length"] = length
        results.append(result_df)

# Combine all results
final_df = pd.concat(results)
pd.set_option("display.max_rows", 500)
print(final_df[["Factor", "Length", "Supertrend"]])
