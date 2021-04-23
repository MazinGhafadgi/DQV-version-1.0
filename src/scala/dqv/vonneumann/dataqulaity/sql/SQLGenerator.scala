package dqv.vonneumann.dataqulaity.sql

import dqv.vonneumann.dataqulaity.config.DQVConfiguration

case class SqlAndMetric(sql: String, metric: String)

object SQLGenerator {

  def generateSQLRule(ruleType: String, ruleValue: String, sourceName: String, sourcePath: String, configRules: DQVConfiguration): String = {
   // val window = s"WHERE ${jobConfig.column} BETWEEN '${jobConfig.startDate}' AND '${jobConfig.endDate}'"
    val reportType = configRules.reportType
    val target = if(sourceName == "BigQuery") sourcePath else sourceName

    if(ruleValue.toLowerCase.startsWith("select")) ruleValue
    else {
      ruleType match {
        case "SizeRule" => size(target)
        case "StatisticsRule"                         => function(ruleValue, target)
        case "Uniqueness"                             => if(reportType == "percentage") uniquenessInPercentage(ruleValue, target) else uniqueness(ruleValue, target)
        case "ProportionMissingInPercentage"          => inPercentage(ruleValue, sourceName)
        case "DateTimeRule"                           => dateRule(ruleValue, target)
        case "EmailRule"                              => validEmailRule(ruleValue, target)
        case "InvalidEmailRule"                       => InValidEmailRule(ruleValue, target)
        case _                                        => if(reportType == "percentage") defaultInPercentage(ruleValue, target) else default(ruleValue, target)
      }
    }
  }

  private def size(target: String)                            = s"SELECT count(*) FROM $target"
  private def function(functionValue: String, target: String) = s"SELECT $functionValue FROM $target"
  private def inPercentage(ruleValue: String, sourceName: String) = s"SELECT CAST (SUM (CASE WHEN $ruleValue is NULL THEN 1 ELSE 0 END) as float) / COUNT(*) AS ProportionMissing FROM $sourceName"
  private def default(filter: String, target: String)                     = s"SELECT count(*) FROM $target where $filter"
  private def defaultInPercentage(filter: String, target: String)         = s"SELECT ((SELECT count(*) FROM $target where $filter) / count(*) ) FROM $target"
  private def uniqueness(column: String, target: String)                  = s"SELECT count(distinct $column) FROM $target"
  private def uniquenessInPercentage(column: String, target: String)      = s"SELECT count(distinct $column)  / COUNT(*) FROM $target"
  private def dateRule(ruleValue: String, target: String) = s"SELECT count(to_date($ruleValue, 'yyyy/mm/dd')) from $target"
  private def validEmailRule(ruleValue: String, target: String) = s"SELECT count(*) from $target where $ruleValue like '%_@__%.__%'"
  private def InValidEmailRule(ruleValue: String, target: String) = s"SELECT count(*) from $target where $ruleValue not like '%_@__%.__%'"
  private def functionAndFilter(functionValue: String, target: String, filter: String) = s"SELECT $functionValue FROM $target where $filter"


}
