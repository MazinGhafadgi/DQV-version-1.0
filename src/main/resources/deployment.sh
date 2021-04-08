#!/usr/bin/env bash

# setting a project
./google-cloud-sdk/bin/gcloud config set project vf-grp-acoe-tst-dfq-01


# Build jar file
sbt clean assembly

# copy Jar file into bucket
./google-cloud-sdk/bin/gsutil cp /target/scala-2.12/Data-quality-tool.jar gs://data-quality-acoe

# create cluster
gcloud beta dataproc clusters create cluster-5cac-test \
--region europe-west1 \
--zone europe-west1-b \
--master-machine-type n1-standard-4 \
--master-boot-disk-size 500 \
--num-workers 2 \
--worker-machine-type n1-standard-4 \
--worker-boot-disk-size 500 \
--image-version 2.0-debian10 \
--project vf-grp-acoe-tst-dfq-01

 ./google-cloud-sdk/bin/gcloud dataproc jobs submit spark \
       --project vf-grp-acoe-tst-dfq-01 \
       --cluster cluster-5cac-test-m \
       --region europe-west1 \
       --class vodafone.dataqulaity.app.DataQualityCheckApp \
       --jars gs://data-quality-acoe/Data-quality-tool-1.0.jar \
       --properties="spark.submit.deployMode=cluster" \
       -- --bucket data-quality-acoe --jsonFile profile.json --mode cluster --column RSVDATE --format yyyy/MM/dd --startDate 2018/04/29 --endDate 2020/01/08

# shutdown the cluster
gcloud dataproc clusters delete cluster-5ac-test --region=europe-west1
