import pandas as pd
import sqlite3
from tqdm import tqdm

def query_db(query, db_path="stock_data.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# # Use tqdm to show the progress bar
# df = pd.read_csv("dataset.csv", low_memory=False)

# df.reset_index(drop=True, inplace=True)
# print("querying prices")
# prices = query_db("select ticker, Date, Close from prices")

# print("merging dfs")

# df = pd.merge(df, prices, how="left", on=["Date", "ticker"])

# print("assigning indicators")

# df["super_12_3_indicator"] = df["Close_x"] > df["SUPERT_12_3.0"]
# df["super_11_2_indicator"] = df["Close_x"] > df["SUPERT_11_2.0"]
# df["super_10_1_indicator"] = df["Close_x"] > df["SUPERT_10_1.0"]

# df.to_csv("prepped_data.csv")

df = pd.read_csv("prepped_data.csv", low_memory=False)
print(df.head())
print("getting tickers")

tickers = query_db('select distinct(ticker) from prices')["ticker"].tolist()

roi_df = pd.DataFrame(columns=["ticker", "buy_price", "sell_price", "buy_date", "sell_date"])

for ticker in tqdm(tickers):
    in_trade = False
    current_trade_price = None
    current_trade_date = None
    sub_df = df[df["ticker"] == ticker]
    sub_df = sub_df.sort_values(by="Date")
    for row in sub_df.itertuples(index=False):
        if not in_trade:
            if row.super_12_3_indicator and row.super_11_2_indicator and row.super_10_1_indicator:
                in_trade = True
                current_trade_price = row.Close_x
                current_trade_date = row.Date
        if in_trade:
            if sum([int(row.super_12_3_indicator), int(row.super_11_2_indicator), int(row.super_10_1_indicator)]) <= 1:
                new_row = pd.DataFrame([{"ticker": ticker, 
                                        "buy_price": current_trade_price, 
                                        "sell_price": row.Close_x, 
                                        "buy_date": current_trade_date, 
                                        "sell_date": row.Date}])
                roi_df = pd.concat([roi_df, new_row], ignore_index=True)
                in_trade = False

roi_df["return"] = (1 - (roi_df["sell_price"] / roi_df["buy_price"])) * 100
roi_df.to_csv("roi.csv")