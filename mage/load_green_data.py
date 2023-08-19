import io
import pandas as pd
import requests
import pyarrow.parquet as pq
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load data from Google Cloud Storage
    """
    green_url = f'https://storage.googleapis.com/nyctaxidata_ss/green_tripdata_2023-01.parquet'
    response = requests.get(green_url)
    green_file = io.BytesIO(response.content)
    green_table = pq.read_table(green_file)
    green_df = green_table.to_pandas() 
    
    return green_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
