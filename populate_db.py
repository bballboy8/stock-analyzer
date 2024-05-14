import sqlite3
import pandas as pd
from tqdm import tqdm
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

def load_tickers(file_path="tickers.txt"):
    with open(file_path, "r") as file:
        return [line.strip() for line in file]

def create_database_connection():
    try:
        return sqlite3.connect("stock_data.db", check_same_thread=False)
    except Exception as e:
        print(f"Error creating database connection: {e}")
        return None

def store_dataframe(dataframe, table_name, conn):
    try:
        dataframe.to_sql(table_name, conn, if_exists='append', index=False)
    except sqlite3.OperationalError as e:
        error_message = str(e)
        if "has no column named" in error_message:
            missing_column = error_message.split("named ")[1].strip()
            if not check_column_exists(conn, table_name, missing_column):
                result = alter_table_to_add_column(conn, table_name, missing_column, dataframe[missing_column].dtype)
                if not result:
                    dataframe.to_sql(table_name, conn, if_exists='append', index=False)
                    return
                store_dataframe(dataframe, table_name, conn)  # Retry storing the dataframe
            else:
                print(f"Attempted to add an existing column {missing_column} to {table_name}")
        else:
            print(f"Error storing data in {table_name}: {error_message}")
    except Exception as e:
        print(f"Unhandled error storing data in {table_name}: {e}")

def check_column_exists(conn, table_name, column_name):
    """ Check if the column exists in the table """
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    return column_name in columns

def alter_table_to_add_column(conn, table_name, column_name, column_type):
    """ Add a missing column to a table in the database """
    try:
        cursor = conn.cursor()
        cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {get_sql_type(column_type)}')
        conn.commit()
        print(f"Added missing column '{column_name}' to '{table_name}'")
        return True
    except sqlite3.OperationalError as e:
        print(f"Error adding column {column_name} to {table_name}: {e}")
        return False

def create_table_from_dataframe(conn, table_name, dataframe):
    """ Creates a table from a DataFrame's schema """
    sql_type_dict = {col: get_sql_type(dtype) for col, dtype in dataframe.dtypes.items()}
    columns_with_types = ", ".join([f"{col} {dtype}" for col, dtype in sql_type_dict.items()])
    create_stmt = f"CREATE TABLE {table_name} ({columns_with_types})"
    conn.execute(create_stmt)
    conn.commit()

def get_sql_type(pandas_type):
    """ Map pandas dtype to SQL dtype """
    if pd.api.types.is_string_dtype(pandas_type):
        return 'TEXT'
    elif pd.api.types.is_numeric_dtype(pandas_type):
        return 'REAL'
    elif pd.api.types.is_datetime64_any_dtype(pandas_type):
        return 'DATETIME'
    return 'BLOB'  # Default to BLOB if type is unclear

def convert_timestamps(dataframe):
    for col in dataframe.columns:
        if pd.api.types.is_datetime64_any_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    return dataframe

def fetch_data_safely(stock: yf.Ticker, attribute):
    try:
        if attribute == "history":
            return stock.history(period="max").reset_index()
        elif attribute == "get_shares_full":
            data = stock.get_shares_full(start="1970-01-01")
            if data is not None:
                data = data.reset_index()  # Reset the index first to turn the index into a column
            else:
                return pd.DataFrame()
            data.columns = ['Date', 'value']  # Rename columns to appropriate names
            return data
        try:
            data = getattr(stock, attribute)
        except Exception as e:
            print(f"Error getting field {attribute} due to {str(e)}")
            return pd.DataFrame()
        if attribute in ["dividends", "splits"]:
            data = data.reset_index()  # Reset the index first to turn the index into a column
            data.columns = ['Date', 'value']  # Rename columns to appropriate names
            return data
        elif attribute in ["quarterly_financials", "quarterly_balance_sheet", "quarterly_cash_flow"]:
            data = data.T
            data = data.reset_index()
            return data
        else:
            data = data.reset_index()
            return data
    except Exception:
        print(f"{attribute} not implemented for {stock.ticker}")
        return pd.DataFrame()

def get_stock_data_with_financials(ticker):
    conn = create_database_connection()
    stock = yf.Ticker(ticker)
    if conn:
        data = {
            'prices': fetch_data_safely(stock, 'history'),
            'dividends': fetch_data_safely(stock, 'dividends'),
            'splits': fetch_data_safely(stock, 'splits'),
            'share_counts': fetch_data_safely(stock, 'get_shares_full'),
            'income_statements': fetch_data_safely(stock, 'quarterly_financials'),
            'balance_sheets': fetch_data_safely(stock, 'quarterly_balance_sheet'),
            'cash_flows': fetch_data_safely(stock, 'quarterly_cash_flow'),
            'insider_transactions': fetch_data_safely(stock, 'insider_transactions'),
            'upgrades_downgrades': fetch_data_safely(stock, 'upgrades_downgrades'),
            'earnings': fetch_data_safely(stock, 'earnings')
        }

        for key, df in data.items():
            if df is not None and not df.empty:
                df['ticker'] = ticker
                df = convert_timestamps(df)
                store_dataframe(df, key, conn)
                    
        conn.close()

def main():
    tickers = load_tickers()
    with ThreadPoolExecutor(max_workers=1000) as executor:
        futures = [executor.submit(get_stock_data_with_financials, ticker) for ticker in tickers]
        for future in tqdm(as_completed(futures), total=len(tickers)):
            future.result()

if __name__ == "__main__":
    main()
