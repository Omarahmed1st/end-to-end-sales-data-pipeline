import pandas as pd

path = "data/100k_Sales_Records.csv"

df = pd.read_csv(path)
print("✅ Rows, Cols:", df.shape)
print("\n✅ Columns:")
print(df.columns.tolist())

print("\n✅ Sample:")
print(df.head(3))

print("\n✅ Nulls (top 10):")
print(df.isna().sum().sort_values(ascending=False).head(10))
