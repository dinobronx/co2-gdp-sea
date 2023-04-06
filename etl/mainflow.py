from create_gcloud_block import create_blocks
from etl_web_to_gcs import etl_web_to_gcs
from etl_gcs_to_bq import etl_gcs_to_bq

from prefect import flow

@flow()
def mainflow(indicators:list[str], countries:list[str], lastXYears:int):
    create_blocks()
    etl_web_to_gcs(indicators=indicators, countries=countries, lastXYears=lastXYears)
    etl_gcs_to_bq()
    

if __name__ == "__main__":
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
    mainflow(indicators=indicators, countries=countries, lastXYears=10)