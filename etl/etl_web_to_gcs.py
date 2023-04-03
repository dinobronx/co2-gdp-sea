from pathlib import Path
import pandas as pd
import requests
import zipfile
import shutil
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

# constant
temp_path = 'temp'
zip_file = f'{temp_path}/wdi.zip'
file_to_extract = 'WDIData.csv'
outfile = f'{temp_path}/economic-analysis.parquet'
bucketpath = 'de-zoomcamp/economic-analysis.parquet'
# can be different
indicators = [
    'CO2 emissions (kt)',
    'GDP (current US$)',
    'GDP growth (annual %)',
    'GDP per capita (current US$)',
    'Population ages 0-14 (% of total population)',
    'Population ages 15-64 (% of total population)',
    'Population ages 65 and above (% of total population)',
    'Population, total',
    'Unemployment, total (% of total labor force) (national estimate)',
    'Net migration'
]
countries = ['Indonesia', 'Vietnam', 'Philippines', 'Singapore', 'Thailand']


@task()
def prep_temp():
    """Create temporary local folder"""
    if not os.path.exists('temp'):
        os.makedirs('temp')
    
@task(log_prints=True)
def download_world_data(dataset_url: str):
    """Downloads world data zip folder"""
    response = requests.get(dataset_url)
    with open(zip_file, 'wb') as f:
        f.write(response.content)
    
@task()
def extract() -> pd.DataFrame:
    """Extracts downloaded zip file and reads into a dataframe"""
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extract(file_to_extract)
    shutil.move(file_to_extract, f'{temp_path}/{file_to_extract}')
    df = pd.read_csv(f'{temp_path}/{file_to_extract}')
    return df
    
    
@task(log_prints=True)
def filter(df: pd.DataFrame, indicators: list[str], countries: list[str], lastXYears: int) -> pd.DataFrame:
    """This task filters indicators, countries and years we are interested in"""
    df = df.loc[df['Indicator Name'].isin(indicators)]
    df = df.loc[df['Country Name'].isin(countries)]
    
    chosen_years = []
    last = 2020
    for i in range(0, lastXYears):
        chosen_years.insert(0, str(last))
        last = last - 1  
    print(f'lastXYears is {lastXYears} then chosen years are {chosen_years}')
    
    chosen_columns = ['Country Name', 'Country Code', 'Indicator Name']
    chosen_columns = chosen_columns + chosen_years
    filtered_df = df.loc[:, chosen_columns]
    return filtered_df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Interpolate missing data"""
    df = df.interpolate()
    null_columns = list(df.columns[df.isnull().any()])
    if len(null_columns) > 0:
        for col in null_columns:
            df[col].bfill(inplace=True)
    print('***** Finding null data *****')
    print(df.isnull().sum(axis=0))
    return df

@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot date data"""
    transformed_df = df.melt(id_vars=['Country Name', 'Country Code', 'Indicator Name'], 
                                  var_name = 'Year', 
                                  value_name='Value')
    print(f'TRANSFORM ${len(transformed_df)}')
    return transformed_df

@task()
def write_local(df: pd.DataFrame, outfile) -> Path:
    """Write DataFrame out as parquet file"""
    pathdf = Path(outfile)
    df.to_parquet(pathdf, compression="gzip")
    return pathdf

@task()
def write_gcs(pathdf: Path) -> None:
    """Uploading local parquet file to gcs"""
    # TODO: Add this in config file
    gcs_block = GcsBucket.load("econ-bucket-creds")
    gcs_block.upload_from_path(
        from_path=pathdf,
        to_path=Path(bucketpath)
    )
    return


@task()
def cleanup():
    """ Clean temporary folders """
    shutil.rmtree('temp')

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    prep_temp()
    dataset_url = 'https://databank.worldbank.org/data/download/WDI_CSV.zip'
    download_world_data(dataset_url)
    raw_df = extract()
    filtered_df = filter(raw_df, indicators=indicators, countries=countries, lastXYears=10)
    clean_df = clean(filtered_df)
    transformed = transform(clean_df)
    pathdf = write_local(transformed, outfile=outfile)
    write_gcs(pathdf=pathdf)
    cleanup()
if __name__ == '__main__':
    etl_web_to_gcs()
    
    



