import functions_framework
from google.cloud import dataproc_v1

@functions_framework.http
def trigger_spark_job(request):
    project_id = "flightproject-479709"                         # <- change
    region = "us-central1"
    cluster_name = "opensky-spark-cluster"

    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": "gs://flightproject-479709-opensky/scripts/gcs_to_bq.py"  # <- change
        },
    }

    result = job_client.submit_job(project_id=project_id, region=region, job=job)
    return f"Submitted job: {result.reference.job_id}"
