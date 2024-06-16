import pandas as pd
import yfinance as yf

# Download historical data for AAPL
ticker = 'AAPL'
data = yf.download(ticker, start='2023-01-01', end='2023-12-31')

# Calculate True Range (TR)
data['Previous_Close'] = data['Close'].shift(1)
data['High-Low'] = data['High'] - data['Low']
data['High-Previous_Close'] = abs(data['High'] - data['Previous_Close'])
data['Low-Previous_Close'] = abs(data['Low'] - data['Previous_Close'])
data['TR'] = data[['High-Low', 'High-Previous_Close', 'Low-Previous_Close']].max(axis=1)

# Calculate Average True Range (ATR)
period = 10
data['ATR'] = data['TR'].rolling(window=period).mean()

# Calculate Basic Upper Band (BU) and Basic Lower Band (BL)
multiplier = 1
data['BU'] = (data['High'] + data['Low']) / 2 + (multiplier * data['ATR'])
data['BL'] = (data['High'] + data['Low']) / 2 - (multiplier * data['ATR'])

# Initialize columns for Final Upper Band (FU), Final Lower Band (FL), and Supertrend
data['FU'] = 0.0
data['FL'] = 0.0
data['Supertrend'] = 0.0

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
        data['Supertrend'][i] = data['FL'][i]
    else:
        data['Supertrend'][i] = data['FU'][i]

# Display the data with the Supertrend calculated
print(data[['Close', 'Supertrend']])
