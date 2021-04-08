# Data Quality @ Vodafone
Data quality can affect all of us, whether determining the accuracy of customer data for sales, marketing or billing. The right quality data supports each of our teams in making optimal commercial decisions.

Some use cases involve a small amount of data, though with the potentially great (adverse) commercial impact. Other use cases involve a large ammount of data also with potentially great commercial impact. Nevertheless, one truth unites use cases. Data quality is a persistent challenge that MUST be managed.

As a persistent challenge, we must look at efficient (yet flexible) means of mastering it. We want the following principles for our own data quality approach:

* a modular approach that can plug-in to other (types of) data pipeline(s)
* an integral set of metrics which can be output to Vodafone approved public cloud datastores (e.g. GCS, BigQuery)
* an integral set of metrics which monitor processing time (cost) for the module
* appropriate (downstream) propagation of defined data quality metrics to downstream steps in a (data) pipeline
* a standards-based metrics structure enabling the use of visualisation tools, alongside post-processing and anomaly detection techniques
* a process for submitting (rather translating) data quality metrics requests from data domain experts (possibly pseudo-SQL?)
* (TBC) a lineage which allows for the tracking of metrics through a pipeline (optional)

Efficient processing should be engineered. Invocation of this efficient processing can be configured and is an approach that has achieved a degree of success amongst the big data community at Vodafone. How the configuration works remains open for discussion at this stage (27/11/2020).

## Directory Structure for THIS Repository
The repository is designed initially for code sharing and collaboration from local markets and group.

1. PLEASE use the [Local Market](https://github.vodafone.com/vfgroup-tsa-datagovernance/Data-Quality/tree/master/Local%20Market) folder to contribute to the data quality discussion.
2. MAIN | PLATFORM | MODULE subfolder will appear under the repository root to bring together the main components of the data quality module.

For further information or questions you have the following resources at your disposal:

* [Data Quality Forum @ Microsoft Teams](https://teams.microsoft.com/l/team/19%3a2dee42f768394cd0b68e015876413089%40thread.tacv2/conversations?groupId=131374e6-fda7-4e59-b333-b3ab0335b4c4&tenantId=68283f3b-8487-4c86-adb3-a5228f18b893), our active discussion forum
* [Data Quality JIRA Project](https://jira.sp.vodafone.com/plugins/servlet/project-config/DQF/summary), GitHub smart commits are enabled.
* [Data Quality Framework Documentation @ Confluence](https://confluence.sp.vodafone.com/x/j5B0Bw), our detailed documentation and design principles.

Where the code within this repository requires documentation to support it's use (and broader adoption), the MVP project will maintain the [wiki](https://github.vodafone.com/vfgroup-tsa-datagovernance/Data-Quality/wiki) for this repository. This is a work-in-progress.


## Running data qulity tool from terminal
## cd Data-Quality
## then run


-b data-quality-acoe -j qaulityRules.json -r profile -m local -c RSVDATE -f yyyy/MM/dd -s 2018/04/29 -e 2020/01/08
-b data-quality-acoe -j assertion.json -m local -c RSVDATE -f yyyy/MM/dd -s 2018/04/29 -e 2020/01/08


sbt "runMain vodafone.dataqulaity.app.DataQualityCheckApp --bucket data-quality-acoe --jsonFile assertion.json --mode cluster --column RSVDATE --format yyyy/MM/dd --startDate 2018/04/29 --endDate 2020/01/08"

##  Or
sbt
runMain vodafone.dataqulaity.app.DataQualityCheckApp -b data-quality-acoe -j profile.json -m local -c RSVDATE -s 2019/04/29 -e 2020/01/08

## buidling the Jar file
sbt clean assembly

## submit spark Job.
./google-cloud-sdk/bin/gcloud dataproc jobs submit spark \
        --project vf-grp-acoe-tst-dfq-01 \
        --cluster cluster-5cac-test-m \
        --region europe-west1 \
        --class vodafone.dataqulaity.app.DataQualityCheckApp \
        --jars gs://data-quality-acoe/Data-quality-tool-1.0.jar \
        --properties="spark.submit.deployMode=cluster" \
        -- --bucket data-quality-acoe --jsonFile profile.json --mode cluster --column RSVDATE --format yyyy/MM/dd --startDate 2018/04/29 --endDate 2020/01/08
