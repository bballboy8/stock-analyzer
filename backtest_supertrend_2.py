# IMPORTING PACKAGES

from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import sqlite3
from tqdm import tqdm
import pandas_ta as ta


def query_db(query, db_path="stock_data.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# SUPERTREND CALCULATION
def get_supertrend(high, low, close, lookback, multiplier):
    
    # ATR
    
    tr1 = pd.DataFrame(high - low)
    tr2 = pd.DataFrame(abs(high - close.shift(1)))
    tr3 = pd.DataFrame(abs(low - close.shift(1)))
    frames = [tr1, tr2, tr3]
    tr = pd.concat(frames, axis = 1, join = 'inner').max(axis = 1)
    atr = tr.ewm(lookback).mean()
    
    # H/L AVG AND BASIC UPPER & LOWER BAND
    
    hl_avg = (high + low) / 2
    upper_band = (hl_avg + multiplier * atr).dropna()
    lower_band = (hl_avg - multiplier * atr).dropna()
    
    # FINAL UPPER BAND
    
    final_bands = pd.DataFrame(columns = ['upper', 'lower'])
    final_bands.iloc[:,0] = [x for x in upper_band - upper_band]
    final_bands.iloc[:,1] = final_bands.iloc[:,0]
    
    for i in range(len(final_bands)):
        if i == 0:
            final_bands.iloc[i,0] = 0
        else:
            if (upper_band[i] < final_bands.iloc[i-1,0]) | (close[i-1] > final_bands.iloc[i-1,0]):
                final_bands.iloc[i,0] = upper_band[i]
            else:
                final_bands.iloc[i,0] = final_bands.iloc[i-1,0]
    
    # FINAL LOWER BAND
    
    for i in range(len(final_bands)):
        if i == 0:
            final_bands.iloc[i, 1] = 0
        else:
            if (lower_band[i] > final_bands.iloc[i-1,1]) | (close[i-1] < final_bands.iloc[i-1,1]):
                final_bands.iloc[i,1] = lower_band[i]
            else:
                final_bands.iloc[i,1] = final_bands.iloc[i-1,1]
    
    # SUPERTREND
    
    supertrend = pd.DataFrame(columns = [f'supertrend_{lookback}'])
    supertrend.iloc[:,0] = [x for x in final_bands['upper'] - final_bands['upper']]
    
    for i in range(len(supertrend)):
        if i == 0:
            supertrend.iloc[i, 0] = 0
        elif supertrend.iloc[i-1, 0] == final_bands.iloc[i-1, 0] and close[i] < final_bands.iloc[i, 0]:
            supertrend.iloc[i, 0] = final_bands.iloc[i, 0]
        elif supertrend.iloc[i-1, 0] == final_bands.iloc[i-1, 0] and close[i] > final_bands.iloc[i, 0]:
            supertrend.iloc[i, 0] = final_bands.iloc[i, 1]
        elif supertrend.iloc[i-1, 0] == final_bands.iloc[i-1, 1] and close[i] > final_bands.iloc[i, 1]:
            supertrend.iloc[i, 0] = final_bands.iloc[i, 1]
        elif supertrend.iloc[i-1, 0] == final_bands.iloc[i-1, 1] and close[i] < final_bands.iloc[i, 1]:
            supertrend.iloc[i, 0] = final_bands.iloc[i, 0]
    
    supertrend = supertrend.set_index(upper_band.index)
    supertrend = supertrend.dropna()[1:]
    
    # ST UPTREND/DOWNTREND
    
    upt = []
    dt = []
    close = close.iloc[len(close) - len(supertrend):]

    for i in range(1, len(supertrend)):
        if close[i] > supertrend.iloc[i, 0]:
            upt.append(supertrend.iloc[i, 0])
            dt.append(np.nan)
        elif close[i] < supertrend.iloc[i, 0]:
            upt.append(np.nan)
            dt.append(supertrend.iloc[i, 0])
        else:
            upt.append(np.nan)
            dt.append(np.nan)
            
    st, upt, dt = pd.Series(supertrend.iloc[:, 0]), pd.Series(upt), pd.Series(dt)
    # upt.index, dt.index = supertrend.index, supertrend.index
    
    return st, upt, dt

def process_ticker(ticker):
    df = query_db(f'select * from prices where ticker = "{ticker}"')
    ticker_df = df.sort_values(by="Date")
    results = []
    for factor, length in [(3, 12), (2, 11), (1, 10)]:
        sub_ticker_df = ta.supertrend(ticker_df['High'], ticker_df['Low'], ticker_df['Close'], length, factor)
        output_df = pd.concat([ticker_df, sub_ticker_df], axis=1)
        results.append(output_df)
    combined_df = pd.concat(results, axis=1)
    combined_df = combined_df.reset_index(drop=True)  # Ensure index is reset
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]  # Remove duplicate columns if any
    return combined_df

def main():
    tickers = query_db('select distinct(ticker) from prices')["ticker"].tolist()
    final_results = []
    
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(process_ticker, ticker): ticker for ticker in tickers}
        for future in tqdm(as_completed(futures), total=len(futures)):
            ticker = futures[future]
            try:
                result = future.result()
                final_results.append(result)
            except Exception as exc:
                print(f'Ticker {ticker} generated an exception: {exc}')
    
    # Ensure all indices are unique before concatenation
    for i, df in enumerate(final_results):
        df['unique_index'] = range(len(df))
        final_results[i] = df.set_index('unique_index')
    
    final_df = pd.concat(final_results, axis=0, ignore_index=True)
    return final_df

# Call the main function and print the last 20 rows of the selected columns
final_df = main()
columns_to_print = ['Date', 'ticker'] + [col for col in final_df.columns if col.startswith('SUPERT_')]
print(final_df[columns_to_print].tail(20))

# Save the final dataframe to a CSV file
final_df.to_csv("dataset.csv", index=False)

