#!/bin/bash
# Run from repository root in Google Cloud Shell. Default prints commands only.
# An authorized mentor/admin can review then use: bash deployment/admin_plan.sh --apply
set -euo pipefail
APPLY=false
if [[ ${1:-} == --apply ]]; then APPLY=true; elif [[ $# != 0 ]]; then exit 2; fi
PROJECT=jcdeah-009
REGION=asia-southeast2
ZONE=asia-southeast2-a
WORKER=daud-dataflow-worker@jcdeah-009.iam.gserviceaccount.com
run() {
  printf '%q ' "$@"
  printf '\n'
  if "$APPLY"; then "$@"; fi
}
# Dedicated resources: no changes to the shared default network.
# Create commands intentionally fail if names already exist: inspect before reusing.
run gcloud compute networks create daud-market --project="$PROJECT" --subnet-mode=custom
run gcloud compute networks subnets create daud-market-jakarta --project="$PROJECT" \
  --network=daud-market --region="$REGION" --range=10.90.0.0/24 --enable-private-ip-google-access
run gcloud compute firewall-rules create daud-market-kafka-from-workers --project="$PROJECT" \
  --network=daud-market --direction=INGRESS --action=ALLOW --rules=tcp:9092 \
  --source-tags=daud-market-worker --target-tags=daud-market-kafka
run gcloud compute firewall-rules create daud-market-workers-internal --project="$PROJECT" \
  --network=daud-market --direction=INGRESS --action=ALLOW --rules=tcp:12345,tcp:12346 \
  --source-tags=daud-market-worker --target-tags=daud-market-worker
run gcloud compute firewall-rules create daud-market-iap-ssh --project="$PROJECT" \
  --network=daud-market --direction=INGRESS --action=ALLOW --rules=tcp:22 \
  --source-ranges=35.235.240.0/20 --target-tags=daud-market-kafka
run gcloud iam service-accounts create daud-dataflow-worker --project="$PROJECT" \
  --display-name='Daud final project Dataflow worker'
run gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$WORKER" --role=roles/dataflow.worker --condition=None
run gcloud storage buckets add-iam-policy-binding gs://jcdeah-009-daud-finalproject \
  --member="serviceAccount:$WORKER" --role=roles/storage.objectAdmin
run bq add-iam-policy-binding --member="serviceAccount:$WORKER" --role=roles/bigquery.dataEditor \
  jcdeah-009:daud_finalproject.stg_market_events
# Broker has no GCP service account. External IP is used for outbound image pulls;
# Kafka binds its private NIC and there is no public 9092 firewall rule.
run gcloud compute instances create daud-market-kafka --project="$PROJECT" --zone="$ZONE" \
  --machine-type=e2-medium --subnet=daud-market-jakarta --private-network-ip=10.90.0.10 \
  --tags=daud-market-kafka --image-family=cos-stable --image-project=cos-cloud \
  --boot-disk-size=20GB --boot-disk-type=pd-standard --no-service-account --no-scopes \
  --metadata=enable-oslogin=TRUE --metadata-from-file=startup-script=deployment/kafka_startup.sh \
  --max-run-duration=2h --instance-termination-action=STOP --no-restart-on-failure \
  --labels=project=daud-finalproject,component=kafka
echo 'Next: grant the submitter actAs on this worker SA and scoped IAP/OS Login access; see deployment guide.'
echo 'No Dataflow job is launched by this script. Existing-name conflicts require review before retry.'
