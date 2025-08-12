# cloud-shell-setup.sh
# This script prepares the environment, creates necessary Terraform files,
# and applies the configuration.
# It sources a separate config.sh file for sensitive variables.
# -----------------------------------------------------------------------------
# IMPORTANT: This script will exit on any error.
# -----------------------------------------------------------------------------
set -e

# --- Sourcing Configuration ---
# Ensure the config file exists and load it.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONFIG_FILE="${SCRIPT_DIR}/config.sh"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file '$CONFIG_FILE' not found."
    echo "Please create a '$CONFIG_FILE' file with your variables."
    exit 1
fi
source "$CONFIG_FILE"

# --- Configuration Validation ---
if [ -z "$PROJECT_ID" ] || [ -z "$BILLING_ACCOUNT_ID" ] || [ -z "$GCS_BUCKET_NAME" ]; then
    echo "ERROR: PROJECT_ID, BILLING_ACCOUNT_ID, and GCS_BUCKET_NAME must be set in '$CONFIG_FILE'."
    exit 1
fi

if [ -z "$NOTIFICATION_CHANNEL_ID" ]; then
    echo "WARNING: The NOTIFICATION_CHANNEL_ID is not set. Budget alerts will be created but notifications will not be enabled."
fi

echo "--- Configuration ---"
echo "Project ID:         $PROJECT_ID"
echo "GCS State Bucket:   $GCS_BUCKET_NAME"
echo "Notification Channel ID: $NOTIFICATION_CHANNEL_ID"
echo "---------------------"

gcloud config set project "$PROJECT_ID"

# Step 1: Enable necessary APIs.
echo "Enabling required APIs..."
gcloud services enable \
  cloudresourcemanager.googleapis.com \
  billingbudgets.googleapis.com \
  storage-component.googleapis.com

# Step 2: Terraform Backend Bucket Creation
# The GCS bucket for the Terraform backend must exist before `terraform init`.
echo "Checking for GCS backend bucket..."
if ! gcloud storage buckets describe "gs://${GCS_BUCKET_NAME}" --format="value(name)" >/dev/null 2>&1; then
  echo "Creating GCS bucket: gs://${GCS_BUCKET_NAME}"
  gcloud storage buckets create "gs://${GCS_BUCKET_NAME}" --project="${PROJECT_ID}" --location="europe-west1" --uniform-bucket-level-access
  echo "Enabling versioning for the bucket..."
  gcloud storage buckets update "gs://${GCS_BUCKET_NAME}" --project="${PROJECT_ID}" --versioning
else
  echo "GCS bucket gs://${GCS_BUCKET_NAME} already exists."
fi

# Step 3: Create Terraform files
echo "Creating Terraform files..."
mkdir -p dogfy-data-sandbox-infra/core
cd dogfy-data-sandbox-infra/core

# Write the Terraform configuration to files
echo "Writing main.tf..."
cat <<'EOF' > main.tf
provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_billing_budget" "project_budget" {
  display_name    = "${var.project_id}-budget"
  billing_account = var.billing_account_id
  amount {
    specified_amount {
      currency_code = "EUR"
      units         = 450
    }
  }
  budget_filter {
    projects = ["projects/${var.project_id}"]
  }
  threshold_rules {
    threshold_percent = 0.5
    spend_basis       = "CURRENT_SPEND"
  }
  threshold_rules {
    threshold_percent = 0.9
    spend_basis       = "CURRENT_SPEND"
  }

  dynamic "all_updates_rule" {
    for_each = var.notification_channel_id != "" ? [1] : []
    content {
      schema_version = "1.0"
      monitoring_notification_channels = [
        "projects/${var.project_id}/notificationChannels/${var.notification_channel_id}"
      ]
    }
  }
}
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

variable "billing_account_id" {
  description = "The ID of the Google Cloud Billing Account."
  type        = string
}

variable "notification_channel_id" {
  description = "The ID of the notification channel for budget alerts."
  type        = string
  default     = "" # Default to an empty string if not provided
}
EOF

echo "Writing backend.tf..."
cat <<EOF > backend.tf
terraform {
  backend "gcs" {
    bucket = "${GCS_BUCKET_NAME}"
    prefix = "env/dev/core"
  }
}
EOF

# Step 4: Initialize and Apply Terraform
echo "Initializing Terraform..."
terraform init

echo "Applying Terraform configuration..."
terraform apply -auto-approve \
  -var="project_id=$PROJECT_ID" \
  -var="billing_account_id=$BILLING_ACCOUNT_ID" \
  -var="notification_channel_id=$NOTIFICATION_CHANNEL_ID"
