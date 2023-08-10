gcloud builds submit --tag gcr.io/apero-data-warehouse/marketing_system_ingest_pipeline
gcloud run deploy marketing-system-ingest-pipeline --image gcr.io/apero-data-warehouse/marketing_system_ingest_pipeline --region us-central1 --allow-unauthenticated --memory=512Mi --concurrency=4
gcloud run services add-iam-policy-binding marketing-system-ingest-pipeline --member=serviceAccount:cloud-run-pubsub-invoker@apero-data-warehouse.iam.gserviceaccount.com --role=roles/run.invoker --region us-central1

gcloud pubsub subscriptions create MarketingSystemIngestPipeline --topic marketing-system-ingest-data  --ack-deadline=600 --push-endpoint=https://marketing-system-ingest-pipeline-fqdj6g73ia-uc.a.run.app/ --push-auth-service-account=cloud-run-pubsub-invoker@apero-data-warehouse.iam.gserviceaccount.com

https://marketing-system-ingest-pipeline-fqdj6g73ia-uc.a.run.app/