resource "google_project_service" "compute" {
  project            = var.project_id
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  project            = var.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iam" {
  project            = var.project_id
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

resource "google_service_account" "pipeline_sa" {
  account_id   = var.service_account_id
  display_name = "JKIA Pipeline Service Account"
  project      = var.project_id
  depends_on   = [google_project_service.iam]
}

resource "google_project_iam_member" "sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

resource "local_file" "sa_key_file" {
  content  = base64decode(google_service_account_key.pipeline_sa_key.private_key)
  filename = "${path.module}/../gcp-sa-key.json"
}

resource "google_storage_bucket" "flight_data" {
  name          = var.gcs_bucket_name
  project       = var.project_id
  location      = var.region
  force_destroy = false

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    enabled = false
  }

  uniform_bucket_level_access = true
  depends_on                  = [google_project_service.storage]
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.bq_raw_dataset
  project                    = var.project_id
  location                   = "US"
  delete_contents_on_destroy = false
  depends_on                 = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = var.bq_analytics_dataset
  project                    = var.project_id
  location                   = "US"
  delete_contents_on_destroy = false
  depends_on                 = [google_project_service.bigquery]
}

resource "google_bigquery_table" "aircraft_states_raw" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "aircraft_states"
  project             = var.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  schema = file("${path.module}/schemas/aircraft_states_raw.json")
  depends_on = [google_bigquery_dataset.raw]
}

resource "google_compute_instance" "airflow_vm" {
  name         = "jkia-airflow-kafka"
  machine_type = var.vm_machine_type
  zone         = var.zone
  project      = var.project_id

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.vm_disk_size_gb
      type  = "pd-standard"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  service_account {
    email  = google_service_account.pipeline_sa.email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = file("${path.module}/startup.sh")

  tags       = ["http-server", "https-server", "airflow"]
  depends_on = [google_project_service.compute]
}

resource "google_compute_firewall" "airflow_web" {
  name    = "allow-airflow-web"
  network = "default"
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  target_tags   = ["airflow"]
  source_ranges = ["0.0.0.0/0"]
  depends_on    = [google_project_service.compute]
}
