import json
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow

@flow()
def create_blocks():
    """ 
    This creates our blocks in prefect. 
    Blocks help us authenticate with gcp just by using our gcp credentials
    """
    
    with open('secrets/de-proj.json') as f, open('config.json') as c:
        secret = json.load(f)
        config = json.load(c)
        # GCP Credentials Block, for uploading to big query
        gcp_cred_block = GcpCredentials(
            service_account_info=secret 
        )

        gcp_cred_block.save("econ-gcs-creds", overwrite=True)

        # GCS Bucket Block, for uploading to GCS
        gcs_bucket_block = GcsBucket(
            gcp_credentials=GcpCredentials.load("econ-gcs-creds"),
            bucket=config['data_lake_bucket'],  
        )

        gcs_bucket_block.save("econ-bucket-creds", overwrite=True)

if __name__ == '__main__':
    create_blocks()