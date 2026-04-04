terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = "finance-zoomcamp"
  region  = "northamerica-northeast2 (Toronto)"
}

resource "google_storage_bucket" "data-lake-bucket" {
  name          = "findata-bucket"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

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


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "finance_data"
  project    = "finance-zoomcamp"
  location   = "US"
}
