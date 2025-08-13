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
