package dqv.vonneumann.dataqulaity.sql

import dqv.vonneumann.dataqulaity.config.DQVConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}


case class SqlAndMetric(sql: String, metric: String)

object SQLGenerator {

  def generateSQLRule(ruleName: String, ruleValue: String, sourceName: String, sourcePath: String, configRules: DQVConfiguration): String = {
   // val window = s"WHERE ${jobConfig.column} BETWEEN '${jobConfig.startDate}' AND '${jobConfig.endDate}'"
    val target = if(sourceName == "BigQuery") sourcePath else sourceName

    if(ruleValue.toLowerCase.startsWith("select")) ruleValue
    else {
      ruleName match {
        case "SizeRule" => size(target)
        case "StatisticsRule"                         => function(ruleValue, target)
        case "Uniqueness"                             => if(configRules.reportType == "percentage") uniquenessInPercentage(ruleValue, target) else uniqueness(ruleValue, target)
        case "ProportionMissingInPercentage"          => inPercentage(ruleValue, sourceName)

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
        case _ => if(configRules.reportType == "percentage") {
          defaultInPercentage(ruleValue, target)
        } else {
          default(ruleValue, target)
        }
      }
    }
  }

  private def size(target: String)                            = s"SELECT count(*) FROM $target"
  private def function(functionValue: String, target: String) = s"SELECT $functionValue FROM $target"
  private def functionAndFilter(functionValue: String, target: String, filter: String) = s"SELECT $functionValue FROM $target where $filter"
  private def inPercentage(ruleValue: String, sourceName: String) = s"SELECT CAST (SUM (CASE WHEN $ruleValue is NULL THEN 1 ELSE 0 END) as float) / COUNT(*) AS ProportionMissing FROM $sourceName"
  private def default(filter: String, target: String)                     = s"SELECT count(*) FROM $target where $filter"
  private def defaultInPercentage(filter: String, target: String)         = s"SELECT ((SELECT count(*) FROM $target where $filter) / count(*) ) FROM $target"
  private def uniqueness(column: String, target: String)                  = s"SELECT count(distinct $column) FROM $target"
  private def uniquenessInPercentage(column: String, target: String)      = s"SELECT count(distinct $column)  / COUNT(*) FROM $target"
}
