import pandas as pd

pd.set_option('display.float_format', '{:.2f}'.format)

df_nyc_taxi = pd.read_csv('2023_Yellow_Taxi_Trip_Data_20260212.csv', low_memory=False)

# print('\n-------------------------------- Total Info --------------------------------\n')
# print(df_nyc_taxi.info())
# print('\n-------------------------------- First 5 rows --------------------------------\n')
# print(df_nyc_taxi.head())
# print('\n-------------------------------- N/A --------------------------------\n')
# print(df_nyc_taxi.isna().sum())

#Cleaning column by column

#VendorID
#print(df_nyc_taxi['VendorID'].unique())

#tpep_pickup_datetime
df_nyc_taxi['tpep_pickup_datetime'] = pd.to_datetime(df_nyc_taxi['tpep_pickup_datetime']) #convert to datetime format
#print(df_nyc_taxi['tpep_pickup_datetime'].unique())

#tpep_dropoff_datetime
df_nyc_taxi['tpep_dropoff_datetime'] = pd.to_datetime(df_nyc_taxi['tpep_dropoff_datetime']) #convert to datetime format
#print(df_nyc_taxi['tpep_dropoff_datetime'].unique())

#Add new column with difference in time between tpep_pickup_datetime and tpep_dropoff_datetime
#diff_pickup/dropoff
df_nyc_taxi['diff_pickup/dropoff'] = df_nyc_taxi['tpep_dropoff_datetime'] - df_nyc_taxi['tpep_pickup_datetime']
df_nyc_taxi['diff_pickup/dropoff'] = df_nyc_taxi['diff_pickup/dropoff'].dt.total_seconds() / 60
#print(df_nyc_taxi[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'diff_pickup/dropoff']].head())

#passenger_count
#print(df_nyc_taxi['passenger_count'].unique()) #check unique values
df_nyc_taxi['passenger_count'] = df_nyc_taxi['passenger_count'].fillna(0).astype(int) #change nan to 0; hange type from float to int
#print(df_nyc_taxi[df_nyc_taxi['passenger_count'].isna()])
#print(df_nyc_taxi['passenger_count'])

#trip_distance
# print(df_nyc_taxi['trip_distance'].unique())
# print(df_nyc_taxi[df_nyc_taxi['trip_distance'].isna()])

#RatecodeID
#print(df_nyc_taxi['RatecodeID'].unique())
df_nyc_taxi['RatecodeID'] = df_nyc_taxi['RatecodeID'].fillna(0).astype(int) #change nan to 0; hange type from float to int
# print(df_nyc_taxi[df_nyc_taxi['RatecodeID'].isna()])
# print(df_nyc_taxi['RatecodeID'])

#store_and_fwd_flag
#print(df_nyc_taxi['store_and_fwd_flag'].unique())

#PULocationID - need to create dict
#print(df_nyc_taxi['PULocationID'].unique())

#DOLocationID - need to create dict
#print(df_nyc_taxi['DOLocationID'].unique())

#payment_type - need to create dict
#print(df_nyc_taxi['payment_type'].unique())

#fare_amount
#print(df_nyc_taxi['fare_amount'].unique())

#extra
#print(df_nyc_taxi['extra'].unique())

#mta_tax
#print(df_nyc_taxi['mta_tax'].unique())

#tip_amount
#print(df_nyc_taxi['tip_amount'].unique())

#tolls_amount
#print(df_nyc_taxi['tolls_amount'].unique())

#improvement_surcharge
#print(df_nyc_taxi['improvement_surcharge'].unique())

#total_amount
#print(df_nyc_taxi['total_amount'].unique())

#congestion_surcharge
#print(df_nyc_taxi['congestion_surcharge'].unique())

#airport_fee
#print(df_nyc_taxi['airport_fee'].unique())

cols_to_fix = ['trip_distance', 'fare_amount', 'tip_amount']

for col in cols_to_fix:
    df_nyc_taxi[col] = pd.to_numeric(df_nyc_taxi[col], errors='coerce')
    df_nyc_taxi[col] = df_nyc_taxi[col].fillna(0)

#is_short_trip - new column 
df_nyc_taxi['is_short_trip'] = (df_nyc_taxi['trip_distance'] < 0.5) | (df_nyc_taxi['diff_pickup/dropoff'] < 2.0)
# print(df_nyc_taxi['is_short_trip'])


#Create connecting to database

from sqlalchemy import create_engine
import urllib
from sqlalchemy import create_engine, event

# Connecting to docker
server = 'localhost,1433'
database = 'NYC_taxi_base' # або твоя БД
username = 'sa'
password = 'SuperPassword91'

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

@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    if executemany:
        cursor.fast_executemany = True


print("Start to load 4 000 000 rows")

# Load data to database
df_nyc_taxi.to_sql(
    'nyc_taxi_trips_pandas', 
    con=engine, 
    if_exists='replace', 
    index=False,
    chunksize=100000 
)

print("Congratulations! All data in database")