import json
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

with open('secrets/de-proj.json') as f:
    secret = json.load(f)
# GCP Credentials Block, for uploading to big query
gcp_cred_block = GcpCredentials(
    # TODO: enter your service account info here
    service_account_info=secret 
)
gcp_cred_block.save("econ-gcs-creds", overwrite=True)

# GCS Bucket Block, for uploading to GCS
gcs_bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("econ-gcs-creds"),
    bucket="de-proj-econ-analysis",  
)

gcs_bucket_block.save("econ-bucket-creds", overwrite=True)