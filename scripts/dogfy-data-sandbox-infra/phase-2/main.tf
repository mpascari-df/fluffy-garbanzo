# Define the Google provider
provider "google" {
  project = var.project_id
  region  = var.region
}

# Create a GCS bucket for data ingestion
resource "google_storage_bucket" "data_bucket" {
  name          = var.gcs_data_bucket_name
  location      = var.region
  project       = var.project_id
  force_destroy = true # WARNING: This allows Terraform to delete the bucket and its contents.
  uniform_bucket_level_access = true
}

# Create a Pub/Sub topic for the MongoDB change stream
resource "google_pubsub_topic" "data_topic" {
  name    = var.pubsub_topic_name
  project = var.project_id
}

# Create a Pub/Sub topic for the dead-letter queue
resource "google_pubsub_topic" "dead_letter_topic" {
  name    = var.pubsub_dead_letter_topic_name
  project = var.project_id
}

# Create a Pub/Sub subscription for the topic
resource "google_pubsub_subscription" "data_subscription" {
  name    = var.pubsub_subscription_name
  topic   = google_pubsub_topic.data_topic.id
  project = var.project_id
  ack_deadline_seconds = 60 # Set a reasonable ack deadline for processing
}

# Create a service account for the Cloud Run service
resource "google_service_account" "data_ingest_sa" {
  account_id   = "data-ingest-sa"
  display_name = "Service Account for Data Ingestion"
  project      = var.project_id
}

# Grant the service account permissions to publish to Pub/Sub
resource "google_project_iam_member" "pubsub_publisher_binding" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.data_ingest_sa.email}"
}

# Grant the service account permissions to subscribe to Pub/Sub
resource "google_project_iam_member" "pubsub_subscriber_binding" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.data_ingest_sa.email}"
}

# Grant the service account permissions to write to the GCS data bucket
resource "google_storage_bucket_iam_member" "data_bucket_binding" {
  bucket = google_storage_bucket.data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.data_ingest_sa.email}"
}

# IMPORTANT: The Cloud Run service itself will be configured manually
# or in a later phase, as it requires a container image to deploy.
# We are only setting up the foundational resources here.
