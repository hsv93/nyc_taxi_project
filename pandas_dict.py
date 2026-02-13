import pandas as pd

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
df_taxi_zones = pd.read_csv(url)

print(df_taxi_zones.head())

dict_payment_type = {
    'payment_type_id': [0, 1, 2, 3, 4],
    'payment_description': ['No charge', 'Credit card', 'Cash', 'Dispute', 'Voided trip']
}

df_payment_types = pd.DataFrame(dict_payment_type)

import urllib
from sqlalchemy import create_engine

# Connecting to docker
server = 'localhost,1433'
database = 'NYC_taxi_base'
username = 'sa'
password = 'PASSWORD'

#Microsoft ODBC Driver
params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    f'TrustServerCertificate=yes;'
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

print("Start to load dict")

# Load data to database
df_payment_types.to_sql(
    'dim_payment_type', 
    con=engine, 
    if_exists='replace', 
    index=False
)

df_taxi_zones.to_sql('dim_zones', con=engine, if_exists='replace', index=False)

print("All dicts in the db !")