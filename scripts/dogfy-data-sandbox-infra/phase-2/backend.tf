terraform {
  backend "gcs" {
    bucket = "dogfy-data-sandbox-tf-state-12345"
    prefix = "env/dev/phase-2"
  }
}
