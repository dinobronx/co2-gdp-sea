from pathlib import Path
import pandas as pd
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

gcs_file = 'economic-analysis.parquet'
temp_path = 'temp'
@task()
def prep_temp():
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

@task()
def extract_from_gcs() -> Path:
    gcs_block = GcsBucket.load('econ-bucket-creds')
    gcs_path = 'de-zoomcamp/economic-analysis.parquet'
    local_path = f'{temp_path}/{gcs_file}'
    gcs_block.get_directory(from_path=gcs_path, local_path=local_path)
    return Path(local_path)

@task()
def read(gcpath: Path) -> pd.DataFrame:
    """ Data cleaning sample """
    df = pd.read_parquet(gcpath)
    # make sure we have no nulls
    print('***** Finding null data *****')
    print(df.isnull().sum(axis=0))
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """ Write data in Big Query """
    gcp_credentials = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table='sea_economic_analysis.rawdata',
        project_id='alien-handler-376020',
        credentials=gcp_credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="replace"
    )

@flow()
def etl_gcs_to_bq() -> None:
    """ Main etl flow to load data to Big Query"""
    prep_temp()
    gcpath = extract_from_gcs()
    df = read(gcpath)
    write_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()