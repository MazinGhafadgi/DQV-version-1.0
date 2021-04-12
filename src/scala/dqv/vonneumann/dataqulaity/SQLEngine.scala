package dqv.vonneumann.dataqulaity

import dqv.vonneumann.dataqulaity.config.DQJobConfig
import org.apache.spark.sql.{DataFrame, SparkSession}


case class SqlAndMetric(sql: String, metric: String)

object RuleSet {

  def generateSQLRule(ruleName: String, ruleValue: String, sourceName: String, sourcePath: String, jobConfig: DQJobConfig): String = {
   // val window = s"WHERE ${jobConfig.column} BETWEEN '${jobConfig.startDate}' AND '${jobConfig.endDate}'"
    val target = if(sourceName == "BigQuery") sourcePath else sourceName

    if(ruleValue.toLowerCase.startsWith("select")) ruleValue
    else {
      ruleName match {
        case "SizeRule" =>
          val sql = size(target)
          println(sql)
          sql
        case "StatisticsRule"                         => function(ruleValue, target)
        case "Uniqueness"                             => uniqueness(ruleValue, target)
        case "ProportionMissingInPercentage"          => s"SELECT CAST (SUM (CASE WHEN $ruleValue is NULL THEN 1 ELSE 0 END) as float) / COUNT(*) AS ProportionMissing FROM $sourceName"

        case "DateTimeRule"   =>  {
          val sql = s"SELECT count(to_date($ruleValue, 'yyyy/mm/dd')) from $target"
          println(sql)
          sql
        }
        case "EmailRule" => {
          val sql = s"SELECT count(*) from $target where $ruleValue like '%_@__%.__%'"
          println(sql)
          sql
        }
        case "InvalidEmailRule" => {
          val sql = s"SELECT count(*) from $target where $ruleValue not like '%_@__%.__%'"
          println(sql)
          sql
        }
        case _ => default(ruleValue, target)
      }
    }
  }


   def excuteRule(sql: String, sparkSession: SparkSession, sourceName: String, sourcePath: String): DataFrame = {
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


 private def size(target: String)                            = s"SELECT count(*) FROM $target"
 private def function(functionValue: String, target: String) = s"SELECT $functionValue FROM $target"
 private def functionAndFilter(functionValue: String, target: String, filter: String) = s"SELECT $functionValue FROM $target where $filter"
 private def uniqueness(column: String, target: String)      = s"SELECT count(distinct $column) FROM $target"
 private def default(filter: String, target: String)         = s"SELECT count(*) FROM $target where $filter"

}
