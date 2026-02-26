variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "gcs_bucket_name" {
  description = "GCS bucket name for raw flight data"
  type        = string
}

variable "bq_raw_dataset" {
  description = "BigQuery dataset for raw ingested data"
  type        = string
  default     = "jkia_raw"
}

variable "bq_analytics_dataset" {
  description = "BigQuery dataset for dbt refined models"
  type        = string
  default     = "jkia_analytics"
}

variable "vm_machine_type" {
  description = "Compute Engine machine type"
  type        = string
  default     = "e2-micro"
}

variable "vm_disk_size_gb" {
  description = "Boot disk size in GB"
  type        = number
  default     = 30
}

variable "service_account_id" {
  description = "Service account ID for pipeline"
  type        = string
  default     = "jkia-pipeline-sa"
}
