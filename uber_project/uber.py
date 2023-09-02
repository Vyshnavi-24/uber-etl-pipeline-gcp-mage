import pandas as pd

df_parq=pd.read_parquet('yellow_tripdata_2023-01.parquet')

df_parq.to_csv('yellow_tripdata_2023-01.csv',index=False)
print(df_parq.head(10))
print(df_parq.info())
    
df_parq = df_parq.drop_duplicates()
df_parq['trip_id'] = df_parq.index

datetime_dim = df_parq[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop = True)

datetime_dim['pickup_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
datetime_dim['pickup_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
datetime_dim['pickup_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
datetime_dim['pickup_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
datetime_dim['pickup_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

datetime_dim['dropoff_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
datetime_dim['dropoff_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
datetime_dim['dropoff_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
datetime_dim['dropoff_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
datetime_dim['dropoff_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

datetime_dim['datetime_id'] = datetime_dim.index

datetime_dim = datetime_dim[['datetime_id','pickup_year','pickup_month','pickup_day',\
 'pickup_hour','pickup_weekday','dropoff_year','dropoff_month','dropoff_day','dropoff_hour','dropoff_weekday']]
print(datetime_dim.head())

passengercount_dim = df_parq[['passenger_count']].reset_index(drop = True)
passengercount_dim['passengercount_id'] = passengercount_dim.index
passengercount_dim = passengercount_dim[['passengercount_id','passenger_count']]
print(passengercount_dim.head())

trip_distance_dim = df_parq[['trip_distance']].reset_index(drop = True)
trip_distance_dim['distance_id'] = trip_distance_dim.index
trip_distance_dim = trip_distance_dim[['distance_id','trip_distance']]
print(trip_distance_dim.head())

RatecodeID_type = {
    1:"Standerd Rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
}

RatecodeID_dim = df_parq[['RatecodeID']].reset_index(drop = True)
RatecodeID_dim['Ratecode_id'] = RatecodeID_dim.index
RatecodeID_dim['RatecodeID_name'] = RatecodeID_dim['RatecodeID'].map(RatecodeID_type)
RatecodeID_dim = RatecodeID_dim[['Ratecode_id','RatecodeID','RatecodeID_name']]
print(RatecodeID_dim.head())

payment_type = {
    1:"Credit card",
    2:"Card",
    3:"No change",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
}
payment_dim = df_parq[['payment_type']].reset_index(drop = True)
payment_dim['payment_id'] = payment_dim.index
payment_dim['payment_name'] = payment_dim['payment_type'].map(payment_type)
payment_dim = payment_dim[['payment_id','payment_type','payment_name']]
print(payment_dim.head())

store_and_fwd_flag_type = {
    'Y': "store and forward trip",
    'N': "Not a store and forward trip"
}
store_and_fwd_flag_dim = df_parq[['store_and_fwd_flag']].reset_index(drop = True)
store_and_fwd_flag_dim['store_and_fwd_flag_id'] = store_and_fwd_flag_dim.index
store_and_fwd_flag_dim['store_and_fwd_flag_name'] = store_and_fwd_flag_dim['store_and_fwd_flag']\
    .map(store_and_fwd_flag_type)
store_and_fwd_flag_dim = store_and_fwd_flag_dim[['store_and_fwd_flag_id','store_and_fwd_flag','store_and_fwd_flag_name']]
print(store_and_fwd_flag_dim.head())

pickup_Location_dim = df_parq[['PULocationID']].reset_index(drop = True)
pickup_Location_dim['pickup_Location_id'] = pickup_Location_dim.index
pickup_Location_dim = pickup_Location_dim[['pickup_Location_id','PULocationID']]
print(pickup_Location_dim.head())

dropoff_location_dim = df_parq[['DOLocationID']].reset_index(drop = True)
dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','DOLocationID']]
print(dropoff_location_dim.head())

fact_table = df_parq.merge(passengercount_dim, left_on='trip_id', right_on='passengercount_id') \
             .merge(trip_distance_dim, left_on='trip_id', right_on='distance_id') \
             .merge(RatecodeID_dim, left_on='trip_id', right_on='Ratecode_id') \
             .merge(pickup_Location_dim, left_on='trip_id', right_on='pickup_Location_id') \
             .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')\
             .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
             .merge(store_and_fwd_flag_dim, left_on='trip_id', right_on='store_and_fwd_flag_id')\
             .merge(payment_dim, left_on='trip_id', right_on='payment_id') \
             [['trip_id','VendorID', 'datetime_id', 'passengercount_id',
               'distance_id', 'Ratecode_id', 'store_and_fwd_flag_id', 'pickup_Location_id', 'dropoff_location_id',
               'payment_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount','congestion_surcharge','airport_fee']]

print(fact_table.head(10))