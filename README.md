# Data Quality @ DQV VON NEUMANN
Data quality can affect all of us, whether determining the accuracy of customer data for sales, marketing or billing. The right quality data supports each of our teams in making optimal commercial decisions.

Some use cases involve a small amount of data, though with the potentially great (adverse) commercial impact. Other use cases involve a large ammount of data also with potentially great commercial impact. Nevertheless, one truth unites use cases. Data quality is a persistent challenge that MUST be managed.

As a persistent challenge, we must look at efficient (yet flexible) means of mastering it. We want the following principles for our own data quality approach:

* a modular approach that can plug-in to other (types of) data pipeline(s)
* an integral set of metrics which can be output to DQV VON NEUMANN approved public cloud datastores (e.g. GCS, BigQuery)
* an integral set of metrics which monitor processing time (cost) for the module
* appropriate (downstream) propagation of defined data quality metrics to downstream steps in a (data) pipeline
* a standards-based metrics structure enabling the use of visualisation tools, alongside post-processing and anomaly detection techniques
* a process for submitting (rather translating) data quality metrics requests from data domain experts (possibly pseudo-SQL?)
* (TBC) a lineage which allows for the tracking of metrics through a pipeline (optional)

Efficient processing should be engineered. Invocation of this efficient processing can be configured and is an approach that has achieved a degree of success amongst the big data community at DQV VON NEUMANN. 

## Running data qulity tool from terminal
## cd Data-Quality
## then run


-b data-quality-acoe -j qaulityRules.json -r profile -m local -c RSVDATE -f yyyy/MM/dd -s 2018/04/29 -e 2020/01/08
-b data-quality-acoe -j assertion.json -m local -c RSVDATE -f yyyy/MM/dd -s 2018/04/29 -e 2020/01/08


sbt "runMain dqv.vonneumann.dataqulaity.app.DataQualityCheckApp runMain dqv.vonneumann.dataqulaity.app.DataQualityCheckApp -b data-quality-acoe -j qaulityRules.json -r profile -m local -c RSVDATE -f yyyy/MM/dd -s 2018/04/29 -e 2020/01/08"

##  Or
sbt
runMain dqv.vonneumann.dataqulaity.app.DataQualityCheckApp -b data-quality-acoe -j qaulityRules.json -r profile -m local -c RSVDATE -f yyyy/MM/dd -s 2018/04/29 -e 2020/01/08

# tools to convert from json to YML.
# https://www.json2yaml.com/

# YAML library.
# https://github.com/circe/circe-yaml

## building the Jar file
sbt clean assembly

## submit spark Job.
./google-cloud-sdk/bin/gcloud dataproc jobs submit spark \
        --project vf-grp-acoe-tst-dfq-01 \
        --cluster cluster-5cac-test-m \
        --region europe-west1 \
        --class dqv.vonneumann.dataqulaity.app.DataQualityCheckApp \
        --jars gs://data-quality-acoe/Data-quality-tool-1.0.jar \
        --properties="spark.submit.deployMode=cluster" \
        -- --bucket data-quality-acoe --jsonFile profile.json --mode cluster --column RSVDATE --format yyyy/MM/dd --startDate 2018/04/29 --endDate 2020/01/08
