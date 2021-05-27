#!/usr/bin/env bash

# setting a project
./google-cloud-sdk/bin/gcloud config set project vf-grp-acoe-tst-dfq-01


# Build jar file
sbt clean assembly

# copy Jar file into bucket
./google-cloud-sdk/bin/gsutil cp /target/scala-2.12/Data-quality-tool.jar gs://data-quality-acoe

# create cluster
gcloud beta dataproc clusters create cluster-bdf6 \
--region europe-west1 \
--zone europe-west1-b \
--master-machine-type n1-standard-4 \
--master-boot-disk-size 500 \
--num-workers 2 \
--worker-machine-type n1-standard-4 \
--worker-boot-disk-size 500 \
--image-version 2.0-debian10 \
--project formal-being-313012

# clusterMame= cluster-bdf6
# project id = formal-being-313012

 gcloud dataproc jobs submit spark \
       --project formal-being-313012 \
       --cluster cluster-23b9 \
       --region europe-west1 \
       --class dqv.vonneumann.dataqulaity.app.DataQualityCheckApp \
       --jars gs://test-dqv-check/Data-quality-tool-1.0.jar \
       -- --bucket test-dqv-check --yml config.yml --mode cluster --column RSVDATE --format yyyy/MM/dd --startDate 2018/04/29 --endDate 2020/01/08

# shutdown the cluster
gcloud dataproc clusters delete cluster-5ac-test --region=europe-west1
