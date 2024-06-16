import pandas as pd

df = pd.read_csv("dataset.csv", nrows=100, low_memory=False)
print(df.head())