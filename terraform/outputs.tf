output "vm_external_ip" {
  description = "External IP of the Airflow/Kafka VM"
  value       = google_compute_instance.airflow_vm.network_interface[0].access_config[0].nat_ip
}

output "gcs_bucket_url" {
  description = "GCS bucket URL for flight data"
  value       = "gs://${google_storage_bucket.flight_data.name}"
}

output "bq_raw_dataset" {
  description = "BigQuery raw dataset ID"
  value       = "${var.project_id}.${google_bigquery_dataset.raw.dataset_id}"
}

output "bq_analytics_dataset" {
  description = "BigQuery analytics dataset ID"
  value       = "${var.project_id}.${google_bigquery_dataset.analytics.dataset_id}"
}

output "service_account_email" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline_sa.email
}
