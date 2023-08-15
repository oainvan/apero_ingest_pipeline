gcloud builds submit --tag gcr.io/pc-api-5581123954802239973-637/apero_trusted_app_subcriptions
gcloud run deploy apero-trusted-app-subcriptions --image gcr.io/pc-api-5581123954802239973-637/apero_trusted_app_subcriptions --region us-central1 --allow-unauthenticated --memory=512Mi --concurrency=5
gcloud run services add-iam-policy-binding apero-trusted-app-subcriptions --member=serviceAccount:cloud-run-pubsub-invoker@pc-api-5581123954802239973-637.iam.gserviceaccount.com --role=roles/run.invoker --region us-central1

gcloud pubsub subscriptions create AperoTrustedAppSubCriptions --topic 	real-time-subcriptions --ack-deadline=600 --push-endpoint=https://apero-trusted-app-subcriptions-mz44gywzkq-uc.a.run.app/ --push-auth-service-account=cloud-run-pubsub-invoker@pc-api-5581123954802239973-637.iam.gserviceaccount.com

 https://marketing-system-ingest-pipeline-2-fqdj6g73ia-uc.a.run.app

