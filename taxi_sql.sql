USE NYC_taxi_base;

IF OBJECT_ID ('dbo.nyc_taxi_trip_SQL' , 'U') IS NOT NULL
	DROP TABLE dbo.nyc_taxi_trip_SQL;

CREATE TABLE dbo.nyc_taxi_trip_SQL (
	VendorID					VARCHAR(50),
	tpep_pickup_datetime		VARCHAR(50),
	tpep_dropoff_datetime		VARCHAR(50),
	passenger_count				VARCHAR(50),
	trip_distance				VARCHAR(50),
	RatecodeID					VARCHAR(50),
	store_and_fwd_flag			NVARCHAR(100),
	PULocationID				VARCHAR(50),
	DOLocationID				VARCHAR(50),
	payment_type				VARCHAR(50),
	fare_amount					VARCHAR(50),
	extra						VARCHAR(50),
	mta_tax						VARCHAR(50),
	tip_amount					VARCHAR(50),
	tolls_amount				VARCHAR(50),
	improvement_surcharge		VARCHAR(50),
	total_amount				NVARCHAR(50),
	congestion_surcharge		VARCHAR(50),
	airport_fee					VARCHAR(50),
-- 	[diff_pickup/dropoff]		FLOAT,
-- 	is_short_trip				INT
 );


BULK INSERT dbo.nyc_taxi_trip_SQL
FROM '/var/opt/mssql/data/2023_Yellow_Taxi_Trip_Data_20260212.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '0x0a',
	TABLOCK
);
