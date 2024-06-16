import pandas as pd
from tqdm import tqdm
import sqlite3

def query_db(query, db_path="stock_data.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def days_between(d1, d2):
    # d1 = datetime.strptime(d1, "%Y-%m-%d")
    # d2 = datetime.strptime(d2, "%Y-%m-%d")
    return (d2 - d1).days

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
    sub_df["sma_5"] = sub_df["Close_x"].rolling(5).mean()
    sub_df["sma_10"] = sub_df["Close_x"].rolling(10).mean()
    sub_df["sma_20"] = sub_df["Close_x"].rolling(20).mean()
    sub_df["sma_60"] = sub_df["Close_x"].rolling(60).mean()
    sub_df["sma_120"] = sub_df["Close_x"].rolling(120).mean()
    for row in sub_df.itertuples(index=False):
        if not in_trade:
            if row.Close_x > row.sma_5 > row.sma_10 > row.sma_20 > row.sma_60 > row.sma_120:
                in_trade = True
                current_trade_price = row.Close_x
                current_trade_date = row.Date
        if in_trade:
            if days_between(current_trade_date, row.Date) >= 10:
                new_row = pd.DataFrame([{"ticker": ticker, 
                                        "buy_price": current_trade_price, 
                                        "sell_price": row.Close_x, 
                                        "buy_date": current_trade_date, 
                                        "sell_date": row.Date}])
                roi_df = pd.concat([roi_df, new_row], ignore_index=True)
                in_trade = False

roi_df["return"] = (1 - (roi_df["sell_price"] / roi_df["buy_price"])) * 100
roi_df.to_csv("roi_shitty_strategy.csv")