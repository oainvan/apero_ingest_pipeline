gcloud builds submit --tag gcr.io/apero-data-warehouse/marketing_system_ingest_pipeline
gcloud run deploy marketing-system-ingest-pipeline-test --image gcr.io/apero-data-warehouse/marketing_system_ingest_pipeline --region us-central1 --allow-unauthenticated --memory=1Gi --concurrency=1 --min-instances 1 --max-instances 30
gcloud run services add-iam-policy-binding marketing-system-ingest-pipeline --member=serviceAccount:cloud-run-pubsub-invoker@apero-data-warehouse.iam.gserviceaccount.com --role=roles/run.invoker --region us-central1

gcloud pubsub subscriptions create MarketingIngestPipelineTest --topic marketing-system-ingest-data  --ack-deadline=600 --push-endpoint=https://marketing-system-ingest-pipeline-test-fqdj6g73ia-uc.a.run.app/ --push-auth-service-account=cloud-run-pubsub-invoker@apero-data-warehouse.iam.gserviceaccount.com

https://marketing-system-ingest-pipeline-test-fqdj6g73ia-uc.a.run.app
