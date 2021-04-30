package dqv.vonneumann.dataqulaity.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object RuleExecutor {
  def executeSQLRule(sql: String, sparkSession: SparkSession, sourceName: String, sourcePath: String): DataFrame = {
    if(sourceName == "BigQuery"){
      val bucket = "gs://dq-test-bucket-1"
      val dataSet = sourcePath.split("\\.")(1)
      sparkSession.conf.set("temporaryGcsBucket", bucket)
      sparkSession.conf.set("viewsEnabled", "true")
      sparkSession.conf.set("materializationDataset", dataSet)
      sparkSession.read.format("bigquery").option("query", sql).load()
    }
    else{
      sparkSession.sql(sql)
    }
  }
}
