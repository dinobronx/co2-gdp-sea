from pathlib import Path
import pandas as pd
import json
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task()
def extract_from_gcs() -> Path:
    """ Extract raw data from gcs """
    gcs_block = GcsBucket.load('econ-bucket-creds')
    gcs_path = 'de-zoomcamp/economic-analysis.parquet'
    gcs_block.get_directory(from_path=gcs_path)
    return Path(gcs_path)

def clean_econ_indicator(indicator: str) -> str:
    """ Rename economic indicator for friendly query """
    if indicator ==  'CO2 emissions (kt)':
        return 'emissions'
    elif indicator == 'GDP (current US$)':
        return 'gdp_real'
    elif indicator == 'GDP growth (annual %)':
        return 'gdp_growth'
    elif indicator == 'GDP per capita (current US$)':
        return 'gdp_percapita'
    elif indicator == 'Net migration':
        return 'net_migration'
    elif indicator == 'Population ages 0-14 (% of total population)':
        return 'population_young'
    elif indicator == 'Population ages 15-64 (% of total population)':
        return 'population_working'
    elif indicator == 'Population ages 65 and above (% of total population)':
        return 'population_old'
    elif indicator == 'Population, total':
        return 'population_total'
    elif indicator == 'Unemployment, total (% of total labor force) (national estimate)':
        return 'unemployment_rate'
    return None

@task()
def transform(gcpath: Path) -> pd.DataFrame:
    """ Data cleaning sample """
    df = pd.read_parquet(gcpath)
    
    # rename column to remove space
    df.rename(columns={'Country Name': 'country', 
                       'Country Code': 'country_code',
                       'Indicator Name': 'econ_indicator', 
                       'Year': 'year', 
                       'Value': 'value'}, inplace=True)
    # make sure we have no nulls
    df['year'] = df['year'].astype(int)
    df['econ_indicator'] = df['econ_indicator'].apply(clean_econ_indicator)
    
    print('***** Finding null data *****')
    print(df.isnull().sum(axis=0))
    print(f'length of downloaded data: {len(df)}')    
    if os.path.exists(gcpath):
      os.rmdir(gcpath)
    
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """ Write data in Big Query """
    gcp_credentials = GcpCredentials.load("econ-gcs-creds")
    schema = [
        {'name':'country', 'type': 'STRING'},
        {'name':'country_code', 'type': 'STRING'},
        {'name':'econ_indicator', 'type': 'STRING'},
        {'name':'year', 'type': 'INTEGER'},
        {'name':'value', 'type': 'FLOAT64'}
    ]
    with open('config.json') as c:
        config = json.load(c)
        df.to_gbq(
            destination_table=f'{config["bq_dataset"]}.rawdata',
            project_id=config["project_id"],
            credentials=gcp_credentials.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="replace",
            table_schema=schema
        )

@flow()
def etl_gcs_to_bq() -> None:
    """ Main etl flow to load data to Big Query"""
    gcpath = extract_from_gcs()
    df = transform(gcpath)
    write_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()