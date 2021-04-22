package dqv.vonneumann.dataqulaity.processtype

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import dqv.vonneumann.dataqulaity.RuleSet.{executeRule, generateSQLRule}
import dqv.vonneumann.dataqulaity.config.{ConfigRules, DQJobConfig}
import dqv.vonneumann.dataqulaity.config.DQJobConfig
import dqv.vonneumann.dataqulaity.output.Metric.metric
object Profiling {

  def profile(configRules: ConfigRules, sparkSession: SparkSession, jobConfig: DQJobConfig) = {

    configRules.ruleNameAndValue.foreach{
      ruleNameAndValue => {
        val ruleName = ruleNameAndValue._1._1.asInstanceOf[String]
        val ruleValue = ruleNameAndValue._1._2.asInstanceOf[String]
        val description = ruleNameAndValue._2.asInstanceOf[String]
        val sql = generateSQLRule(ruleName, ruleValue, configRules.sourceType, configRules.sourcePath, configRules)
        lazy val df = executeRule(sql, sparkSession, configRules.sourceType, configRules.sourcePath)
        lazy val metricValue = metricResult(df.collect().head.get(0), configRules.reportType, ruleName)
        lazy val metricMap = Map("Metric-id"     -> ruleName,
                                 "Rule"          -> ruleValue,
                                 "Metric-result" -> metricValue,
                                 "SourceType"    -> configRules.sourceType,
                                 "SourcePath"    -> configRules.sourcePath,
                                 "Description"   -> description,
                                 "SubmissionDateTime" -> new DateTime().toString("yyyy-MM-dd HH:mm:ss"))

 /*       val columns = Array("Description", "SubmissionDateTime", "Rule-id", "RuleValue", "RuleResult", "SourceType", "SourcePath")
        val values = Array( description, new DateTime().toString("yyyy-MM-dd HH:mm:ss"), ruleName, ruleValue, metricResult(df.collect().head.get(0), ruleName), configRules.sourceType, configRules.sourcePath)
*/

        if(ruleName == "ProportionMissingInPercentage" && metricValue.substring(0, metricValue.length -1).toDouble > 10.0) Console.out.println(Console.RED_B + ruleName + Console.RESET )
        else Console.out.println(Console.GREEN_B + ruleName + Console.RESET )
        metric(metricMap, configRules.sinkType)
        println(" \n")
      }
    }
  }


  def metricResult(value: Any, reportType: String, ruleName: String) = {
    if(reportType == "percentage" && ruleName != "StatisticsRule"){
      val inPercent = value.asInstanceOf[Double] * 100
      val formatToTwoDigits = BigDecimal(inPercent).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      formatToTwoDigits + "%"
    }
    else if(value.isInstanceOf[Long]) value.asInstanceOf[Long].toString
    else if(value.isInstanceOf[Double]) value.asInstanceOf[Double].toString
    else value.asInstanceOf[Int].toString
  }


/*  def profileForBigQuery(configRules: ConfigRules, dfr: DataFrameReader, dataQualityProcessType: DataQualityProcessType) = {
    println(s"----- $dataQualityProcessType Result-----")

    println("---------------------------------------------------------------------------------------------------------------------------------------------------")
    println("%1s".format("|RuleName|") + "%25s".format("|RuleValue|") + "%45s".format("|Value|") + "%25s".format("|SourceType|") +  "%30s".format("|SourcePath|"))
    println("---------------------------------------------------------------------------------------------------------------------------------------------------")
    configRules.ruleNameAndValue.foreach{
      ruleNameAndValue => {
        val ruleName = ruleNameAndValue._1
        val ruleValue = ruleNameAndValue._2
        val sql = generateBigQuerySQLForProfiling(ruleName, ruleValue, configRules.sourcePath)
        println("?????????????????????????????????????????", sql)
        val df = dfr.option("query", sql).load()
        ruleName match {
          case "MaxRule" | "MinRule" | "AVGRule" | "SumRule" | "MeanRule" => {
            val str = f"${ruleName}%-25s   ${ruleValue}%-45s   ${df.collect().head.get(0)}%-15s   ${configRules.sourceName}%-15s   ${configRules.sourcePath}"
            println(str)
          }

          case "isComplete" => {
            val res = if(df.collect().head.get(0).asInstanceOf[Long] > 0) "Failure" else "Success"
            val str = f"${ruleName}%-25s   ${ruleValue}%-45s   $res%-15s   ${configRules.sourceName}%-15s   ${configRules.sourcePath}"
            println(str)
          }

          case "Uniqueness" => {
            val res = if(df.collect().head.get(0).asInstanceOf[Integer] == 1) "Success" else "Failure"
            val str = f"${ruleName}%-25s   ${ruleValue}%-45s   $res%-15s   ${configRules.sourceName}%-15s   ${configRules.sourcePath}"
            println(str)
          }

          case _ =>  {
            val str = f"${ruleName}%-25s   ${ruleValue}%-45s   ${ df.collect().head.get(0)}%-15s   ${configRules.sourceName}%-15s   ${configRules.sourcePath}"
            println(str)
          }
        }
        println("\n")
      }
    }
  }*/

}
