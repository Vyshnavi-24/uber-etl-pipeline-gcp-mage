import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    print(df.info())
    df_parq=df
    df_parq['tpep_pickup_datetime'] = pd.to_datetime(df_parq['tpep_pickup_datetime'])
    df_parq['tpep_dropoff_datetime'] = pd.to_datetime(df_parq['tpep_dropoff_datetime'])
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

    passengercount_dim = df_parq[['passenger_count']].reset_index(drop = True)
    passengercount_dim['passengercount_id'] = passengercount_dim.index
    passengercount_dim = passengercount_dim[['passengercount_id','passenger_count']]

    trip_distance_dim = df_parq[['trip_distance']].reset_index(drop = True)
    trip_distance_dim['distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['distance_id','trip_distance']]

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

    store_and_fwd_flag_type = {
        'Y': "store and forward trip",
        'N': "Not a store and forward trip"
    }
    store_and_fwd_flag_dim = df_parq[['store_and_fwd_flag']].reset_index(drop = True)
    store_and_fwd_flag_dim['store_and_fwd_flag_id'] = store_and_fwd_flag_dim.index
    store_and_fwd_flag_dim['store_and_fwd_flag_name'] = store_and_fwd_flag_dim['store_and_fwd_flag']\
        .map(store_and_fwd_flag_type)
    store_and_fwd_flag_dim = store_and_fwd_flag_dim[['store_and_fwd_flag_id','store_and_fwd_flag','store_and_fwd_flag_name']]

    pickup_Location_dim = df_parq[['PULocationID']].reset_index(drop = True)
    pickup_Location_dim['pickup_Location_id'] = pickup_Location_dim.index
    pickup_Location_dim = pickup_Location_dim[['pickup_Location_id','PULocationID']]

    dropoff_location_dim = df_parq[['DOLocationID']].reset_index(drop = True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','DOLocationID']]

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
    print(fact_table.info())

    """return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "passenger_count_dim":passengercount_dim.to_dict(orient="dict"),
    "trip_distance_dim":trip_distance_dim.to_dict(orient="dict"),
    "rate_code_dim":RatecodeID_dim.to_dict(orient="dict"),
    "pickup_location_dim":pickup_Location_dim.to_dict(orient="dict"),
    "dropoff_location_dim":dropoff_location_dim.to_dict(orient="dict"),
    "store_and_fwd_flag_dim":store_and_fwd_flag_dim.to_dict(orient="dict"),
    "payment_type_dim":payment_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}"""
    return dropoff_location_dim


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
