#!/bin/bash

# Get the project number of our current project:
export PROJECT_NUMBER=`gcloud projects describe $GOOGLE_CLOUD_PROJECT --format='value(projectNumber)'`

# Grant the "Eventarc Event Receiver" IAM role to your project's default Compute Engine service account:
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member=serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com --role=roles/eventarc.eventReceiver

# Grant the "Document AI API User" IAM role to your project's default Compute Engine service account:
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member=serviceAccount:$PROJECT_NUMBER-compute@developer.gserviceaccount.com --role="roles/documentai.apiUser"

# Grant the "Service Account Token Creator" IAM role to the Pub/Sub service agent for your project:
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member=serviceAccount:service-$PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com --role=roles/iam.serviceAccountTokenCreator
