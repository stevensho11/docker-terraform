terraform {
    required_providers {
      google = {
        source = "hashicorp/google"
      }
    }
}

provider "google" {
  project     = "terraform-demo-461702"
  region      = "us-east4"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "terraform-demo-461702-demo-bucket"
  location      = "US"
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}