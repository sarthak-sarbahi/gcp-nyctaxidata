import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(yellow_pd, green_pd):
    """
    Perform transformation steps
    """
    green_df = green_pd[
    ['VendorID', 
     'lpep_pickup_datetime', 
     'lpep_dropoff_datetime', 
     'store_and_fwd_flag', 
     'RatecodeID', 
     'PULocationID', 
     'DOLocationID', 
     'passenger_count', 
     'trip_distance', 
     'fare_amount', 
     'extra', 
     'mta_tax', 
     'tip_amount', 
     'tolls_amount', 
     'improvement_surcharge', 
     'total_amount', 
     'payment_type', 
     'congestion_surcharge']
     ]
    green_rename = {
    "VendorID": "vendor_id",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "drop_datetime",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "drop_location_id"
    }
    green_df.rename(columns=green_rename, inplace=True)
    green_df["taxi_type"] = "green"
    green_df["pickup_datetime"] = pd.to_datetime(green_df["pickup_datetime"])
    green_df["drop_datetime"] = pd.to_datetime(green_df["drop_datetime"])

    yellow_df = yellow_pd[
    ['VendorID', 
    'tpep_pickup_datetime', 
    'tpep_dropoff_datetime', 
    'passenger_count', 
    'trip_distance', 
    'RatecodeID', 
    'store_and_fwd_flag', 
    'PULocationID', 
    'DOLocationID', 
    'payment_type', 
    'fare_amount', 
    'extra', 
    'mta_tax', 
    'tip_amount', 
    'tolls_amount', 
    'improvement_surcharge', 
    'total_amount', 
    'congestion_surcharge']
    ]
    yellow_rename = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "drop_datetime",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "drop_location_id"
    }
    yellow_df.rename(columns=yellow_rename, inplace=True)
    yellow_df["taxi_type"] = "green"
    yellow_df["pickup_datetime"] = pd.to_datetime(yellow_df["pickup_datetime"])
    yellow_df["drop_datetime"] = pd.to_datetime(yellow_df["drop_datetime"])    

    df_union = pd.concat([green_df, yellow_df], ignore_index=True)

    pre_fact = df_union[["vendor_id","passenger_count","trip_distance","store_and_fwd_flag","taxi_type"]].drop_duplicates().reset_index(drop=True)
    pre_fact['trip_id'] = pre_fact.index

    pickup_dim = df_union[["pickup_datetime","pickup_location_id"]].drop_duplicates().reset_index(drop=True)
    pickup_dim['pickup_id'] = pickup_dim.index

    drop_dim = df_union[["drop_datetime","drop_location_id"]].drop_duplicates().reset_index(drop=True)
    drop_dim['drop_id'] = drop_dim.index

    payment_dim = df_union[["rate_code_id","payment_type","fare_amount","extra","mta_tax","improvement_surcharge","tip_amount","tolls_amount","total_amount","congestion_surcharge"]].drop_duplicates().reset_index(drop=True)
    payment_dim["payment_id"] = payment_dim.index            

    fact_table = pre_fact.merge(pickup_dim, left_on='trip_id', right_on='pickup_id') \
             .merge(drop_dim, left_on='trip_id', right_on='drop_id') \
             .merge(payment_dim, left_on='trip_id', right_on='payment_id') \
             [["vendor_id","pickup_id","drop_id","passenger_count","trip_distance","payment_id","store_and_fwd_flag","taxi_type","trip_id"]]

    return {
        "fact_table": fact_table.to_dict(orient="dict"),
        "pickup_dim": pickup_dim.to_dict(orient="dict"),
        "drop_dim": drop_dim.to_dict(orient="dict"),
        "payment_dim": payment_dim.to_dict(orient="dict")
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
