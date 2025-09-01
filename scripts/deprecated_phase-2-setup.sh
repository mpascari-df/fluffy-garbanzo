# phase-2-setup.sh
# This script provisions the core data ingestion infrastructure using Terraform,
# including a GCS bucket, Pub/Sub topic and subscription, and a service account.
# -----------------------------------------------------------------------------
# IMPORTANT: This script will exit on any error.
# -----------------------------------------------------------------------------
set -e

# --- Sourcing Configuration ---
# Ensure the config file exists and load it.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONFIG_FILE="${SCRIPT_DIR}/config_prod.sh"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file '$CONFIG_FILE' not found."
    echo "Please create a '$CONFIG_FILE' file with your variables."
    exit 1
fi
source "$CONFIG_FILE"

# --- Configuration Validation ---
if [ -z "$PROJECT_ID" ] || [ -z "$GCS_BUCKET_NAME" ] || [ -z "$GCS_DATA_BUCKET_NAME" ] || [ -z "$PUBSUB_TOPIC_NAME" ] || [ -z "$PUBSUB_SUBSCRIPTION_NAME" ] || [ -z "$PUBSUB_DEAD_LETTER_TOPIC_NAME" ]; then
    echo "ERROR: PROJECT_ID, GCS_BUCKET_NAME, GCS_DATA_BUCKET_NAME, PUBSUB_TOPIC_NAME, PUBSUB_SUBSCRIPTION_NAME, and PUBSUB_DEAD_LETTER_TOPIC_NAME must be set in '$CONFIG_FILE'."
    exit 1
fi

echo "--- Configuration for Phase 2 ---"
echo "Project ID:           $PROJECT_ID"
echo "GCS State Bucket:     $GCS_BUCKET_NAME"
echo "GCS Data Bucket:      $GCS_DATA_BUCKET_NAME"
echo "Pub/Sub Topic:        $PUBSUB_TOPIC_NAME"
echo "Pub/Sub DLQ Topic:    $PUBSUB_DEAD_LETTER_TOPIC_NAME"
echo "Pub/Sub Subscription: $PUBSUB_SUBSCRIPTION_NAME"
echo "---------------------------------"

gcloud config set project "$PROJECT_ID"

# Step 1: Enable necessary APIs.
echo "Enabling required APIs..."
gcloud services enable \
  iam.googleapis.com \
  run.googleapis.com \
  pubsub.googleapis.com \
  storage-component.googleapis.com

# Step 2: Create Terraform files
echo "Creating Terraform files for Phase 2..."
mkdir -p dogfy-data-sandbox-infra/phase-2
cd dogfy-data-sandbox-infra/phase-2

# Write the Terraform configuration to files
echo "Writing main.tf..."
cat <<'EOF' > main.tf
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
EOF

echo "Writing variables.tf..."
cat <<'EOF' > variables.tf
variable "project_id" {
  description = "The ID of the Google Cloud Project."
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources in."
  type        = string
  default     = "europe-west1"
}

variable "gcs_data_bucket_name" {
  description = "The globally unique name for the GCS bucket to store ingested data."
  type        = string
}

variable "pubsub_topic_name" {
  description = "The name for the Pub/Sub topic."
  type        = string
}

variable "pubsub_subscription_name" {
  description = "The name for the Pub/Sub subscription."
  type        = string
}

variable "pubsub_dead_letter_topic_name" {
  description = "The name for the Pub/Sub dead-letter topic."
  type        = string
}
EOF

echo "Writing backend.tf..."
cat <<EOF > backend.tf
terraform {
  backend "gcs" {
    bucket = "${GCS_BUCKET_NAME}"
    prefix = "env/dev/phase-2"
  }
}
EOF

# Step 3: Initialize and Apply Terraform
echo "Initializing Terraform..."
terraform init

echo "Applying Terraform configuration..."
terraform apply -auto-approve \
  -var="project_id=$PROJECT_ID" \
  -var="gcs_data_bucket_name=$GCS_DATA_BUCKET_NAME" \
  -var="pubsub_topic_name=$PUBSUB_TOPIC_NAME" \
  -var="pubsub_subscription_name=$PUBSUB_SUBSCRIPTION_NAME" \
  -var="pubsub_dead_letter_topic_name=$PUBSUB_DEAD_LETTER_TOPIC_NAME"
