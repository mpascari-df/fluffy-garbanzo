MongoDB Pipeline Monitoring
This directory contains monitoring infrastructure for the MongoDB ingestion pipeline.

Overview
The monitoring solution tracks data flow through:

MongoDB → Cloud Run Ingestor → Pub/Sub → Cloud Function → GCS
Components
1. Log-Based Metrics (metrics/)
Creates metrics from structured logs (METRIC: prefixed):

Ingestion Metrics:

change_stream_events - MongoDB events processed
pubsub_published - Messages published to Pub/Sub
pubsub_publish_failed - Failed publishes
slow_publishes - Publishes over 1 second
ingestor_system_stats - Periodic health stats
Storage Metrics:

function_messages_received - Messages received by function
gcs_write_success - Successful GCS writes
gcs_write_failed - Failed GCS writes
function_stats - Periodic function stats
Error Metrics:

format_errors - Data format issues
unexpected_errors - Unexpected failures
data_loss_events - Critical data loss detection
Performance Metrics:

high_latency_operations - Operations over 1000ms
events_by_collection - Events grouped by MongoDB collection
2. Dashboards (dashboards/) - Coming Soon
Will include:

Operational Dashboard - Service health and performance
Data Insights Dashboard - Collection patterns and volumes
Executive Dashboard - High-level KPIs
3. Alerts (alerts/) - Coming Soon
Critical alerts for:

Data loss events
Pipeline stalls
High error rates
4. Utilities (utils/)
verify_monitoring.sh - Validates monitoring setup
Quick Start
Deploy Everything
bash
# From repository root
./scripts/phase-7-deploy-monitoring.sh
Or manually:

bash
# From monitoring directory
./setup_all.sh
Create Metrics Only
bash
./metrics/create_log_metrics.sh
Verify Setup
bash
./utils/verify_monitoring.sh
Clean Up
bash
./metrics/delete_log_metrics.sh
Viewing Metrics
Cloud Console
Metrics List:
   https://console.cloud.google.com/logs/metrics?project=PROJECT_ID
Metrics Explorer:
   https://console.cloud.google.com/monitoring/metrics-explorer?project=PROJECT_ID
Resource Type: Logging
Metrics: logging.googleapis.com/user/*
Command Line
bash
# List all metrics
gcloud logging metrics list --project=PROJECT_ID

# View recent metric events
gcloud logging read 'textPayload:"METRIC:"' \
  --project=PROJECT_ID \
  --limit=50

# Count events by type
gcloud logging read 'textPayload:"METRIC:"' \
  --project=PROJECT_ID \
  --format="value(textPayload)" | \
  grep -o "METRIC:[a-z_]*" | sort | uniq -c
Metric Details
Data Flow Metrics
Metric	Description	Source
change_stream_events	MongoDB changes received	Ingestor
pubsub_published	Messages sent to Pub/Sub	Ingestor
gcs_write_success	Files written to GCS	Function
Performance Metrics
Metric	Description	Normal Range
slow_publishes	Pub/Sub > 1s	< 1% of total
high_latency_operations	Any op > 1000ms	< 5% of total
Error Metrics
Metric	Description	Alert Threshold
data_loss_events	Critical data loss	> 0
format_errors	Bad data format	> 10/min
unexpected_errors	System errors	> 5/min
Troubleshooting
No Metrics Data
Check services are running:
bash
   ./utils/verify_monitoring.sh
Check logs are being generated:
bash
   gcloud logging read 'textPayload:"METRIC:"' \
     --project=PROJECT_ID --limit=10
Wait 2-3 minutes after metric creation for data to appear
Missing Collections
The events_by_collection metric only tracks known collections:

customers
leads
changelogs
sysusers
users-metadata
Add more in metrics/create_log_metrics.sh

High Error Rates
Check specific error types:

bash
# Format errors
gcloud logging read 'textPayload:"METRIC:format_error"' \
  --project=PROJECT_ID --limit=5

# Unexpected errors
gcloud logging read 'textPayload:"METRIC:unexpected_error"' \
  --project=PROJECT_ID --limit=5
Next Steps
Create Dashboards - Use Metrics Explorer to build custom dashboards
Set Up Alerts - Configure notification channels and alert policies
Add SLOs - Define Service Level Objectives for critical metrics
Enable Cloud Trace - Add distributed tracing for deeper insights
Support
For issues or questions:

Check service logs
Run ./utils/verify_monitoring.sh
Review this README
