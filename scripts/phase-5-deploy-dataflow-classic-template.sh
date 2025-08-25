#!/bin/bash
#
# This script builds and deploys the Dataflow Classic Template.
# It creates a template file and stages it to GCS.
#
# -----------------------------------------------------------------------------
# IMPORTANT: This script will exit on any error.
# -----------------------------------------------------------------------------
set -e

# --- Sourcing Configuration ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONFIG_FILE="${SCRIPT_DIR}/config.sh"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file '$CONFIG_FILE' not found."
    exit 1
fi
source "$CONFIG_FILE"

# --- Configuration Validation ---
if [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$GCS_DATAFLOW_BUCKET_NAME" ] || [ -z "$DATAFLOW_TEMPLATE_NAME" ]; then
    echo "ERROR: Required variables must be set in '$CONFIG_FILE'."
    echo "Please set: PROJECT_ID, REGION, GCS_DATAFLOW_BUCKET_NAME, DATAFLOW_TEMPLATE_NAME"
    exit 1
fi

# --- Template Configuration ---
# The template file will be named with the current date to ensure uniqueness.
TEMPLATE_FILE_GCS_PATH="gs://${GCS_DATAFLOW_BUCKET_NAME}/dataflow/templates/${DATAFLOW_TEMPLATE_NAME}-$(date +%Y%m%d-%H%M%S)"
STAGING_LOCATION="gs://${GCS_DATAFLOW_BUCKET_NAME}/dataflow/staging"

echo "--- Configuration ---"
echo "Project ID:           $PROJECT_ID"
echo "Region:               $REGION"
echo "GCS Dataflow Bucket:  $GCS_DATAFLOW_BUCKET_NAME"
echo "Template Name:        $DATAFLOW_TEMPLATE_NAME"
echo "Template GCS Path:    $TEMPLATE_FILE_GCS_PATH"
echo "---------------------"

# Step 1: Ensure the GCS bucket exists
echo "Checking for Dataflow GCS bucket..."
gcloud storage buckets describe "gs://${GCS_DATAFLOW_BUCKET_NAME}" --project="$PROJECT_ID" >/dev/null 2>&1 || {
    echo "ERROR: GCS bucket '${GCS_DATAFLOW_BUCKET_NAME}' not found."
    exit 1
}

# Step 2: Navigate to the directory containing the pipeline code and set up the virtual environment.
echo "Navigating to pipeline directory..."
cd "${SCRIPT_DIR}/../dataflow"

# Create a virtual environment if it doesn't exist and activate it
if [ ! -d "dataflow-classic-venv" ]; then
    echo "Creating virtual environment 'dataflow-classic-venv'..."
    python3 -m venv dataflow-classic-venv
fi

echo "Activating virtual environment and installing dependencies..."
source dataflow-classic-venv/bin/activate
pip install -r requirements.txt

# Step 3: Build the Dataflow Classic Template.
echo "Building Dataflow Classic Template..."
python3 -m main \
    --runner=DataflowRunner \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --staging_location="$STAGING_LOCATION" \
    --template_location="$TEMPLATE_FILE_GCS_PATH" \
    --setup_file="./setup.py" \
    --requirements_file="./requirements.txt" \
    --save_main_session

echo "--- Deployment Complete ---"
echo "Dataflow Classic Template created successfully."
echo "Template file is located at: $TEMPLATE_FILE_GCS_PATH"
echo "You can now run this template."
echo "---------------------"

# Deactivate the virtual environment
deactivate
