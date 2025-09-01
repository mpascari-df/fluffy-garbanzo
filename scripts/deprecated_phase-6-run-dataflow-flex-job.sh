#!/bin/bash
#
# This script runs the Dataflow Flex Template job.
# It sources configuration from config.sh and passes the required GCS paths
# as parameters to the Dataflow job.
#
# -----------------------------------------------------------------------------
set -e

# --- Sourcing Configuration ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONFIG_FILE="${SCRIPT_DIR}/config_prod.sh"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file '$CONFIG_FILE' not found."
    exit 1
fi
source "$CONFIG_FILE"

# --- Configuration Validation ---
if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$GCS_DATA_BUCKET_NAME" ] || [ -z "$GCS_DATAFLOW_BUCKET_NAME" ] || [ -z "$DATAFLOW_TEMPLATE_NAME" ] || [ -z "$DATAFLOW_JOB_NAME_PREFIX" ] || [ -z "$DATAFLOW_SA" ]; then
    echo "ERROR: Required variables must be set in '$CONFIG_FILE'."
    echo "Please set: PROJECT_ID, REGION, GCS_DATA_BUCKET_NAME, GCS_DATAFLOW_BUCKET_NAME, DATAFLOW_TEMPLATE_NAME, DATAFLOW_JOB_NAME_PREFIX, DATAFLOW_SA"
    exit 1
fi

# --- Job Configuration ---
JOB_NAME="${DATAFLOW_JOB_NAME_PREFIX}-$(date +%Y%m%d-%H%M%S)"

# --- GCS Paths ---
# Path to the Flex Template metadata file created by the deployment script.
TEMPLATE_PATH="gs://${GCS_DATAFLOW_BUCKET_NAME}/dataflow/templates/${DATAFLOW_TEMPLATE_NAME}.json"

# Input for raw JSON files. The wildcard '*' ensures it reads from all subdirectories.
INPUT_PATH="gs://${GCS_DATA_BUCKET_NAME}/raw/customers/*/*"

# Output for the processed Parquet files.
OUTPUT_PATH="gs://${GCS_DATAFLOW_BUCKET_NAME}/processed/"

# Output for records that fail transformation.
DEAD_LETTER_PATH="gs://${GCS_DATAFLOW_BUCKET_NAME}/dead-letter/"

# Staging location for Dataflow to store temporary files.
STAGING_PATH="gs://${GCS_DATAFLOW_BUCKET_NAME}/staging/"

echo "--- Starting Dataflow Job: ${JOB_NAME} ---"

gcloud dataflow flex-template run "$JOB_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --template-file-gcs-location="$TEMPLATE_PATH" \
    --staging-location="$STAGING_PATH" \
    --service-account-email="$DATAFLOW_SA" \
    --parameters input="$INPUT_PATH" \
    --parameters output="$OUTPUT_PATH" \
    --parameters dead_letter_output="$DEAD_LETTER_PATH"

echo "--- Job started successfully. ---"
echo "Monitor its progress in the GCP Console: https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}"
