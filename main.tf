terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.60.1"
    }
  }
}

locals {
    local_data = jsondecode(file("${path.module}/config.json"))
}

provider "google" {
  project = local.local_data.project_id
  region = local.local_data.region
  credentials = file(local.local_data.secret_path)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

resource "google_storage_bucket" "data-lake" {
  name          = local.local_data.data_lake_bucket
  location      = local.local_data.region

  storage_class = local.local_data.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dwh" {
  dataset_id = local.local_data.bq_dataset
  project    = local.local_data.project_id
  location   = local.local_data.region
}

resource "google_bigquery_dataset" "dbt" {
  dataset_id = local.local_data.bq_ds_for_dbt
  project    = local.local_data.project_id
  location   = local.local_data.region
}