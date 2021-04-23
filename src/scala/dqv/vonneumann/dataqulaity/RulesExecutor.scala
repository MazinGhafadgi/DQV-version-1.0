package dqv.vonneumann.dataqulaity

import dqv.vonneumann.dataqulaity.config.{DQVConfiguration, DQJobConfig}
import dqv.vonneumann.dataqulaity.metric.Metric.metricGenerator
import dqv.vonneumann.dataqulaity.sql.RuleExecutor.executeSQLRule
import dqv.vonneumann.dataqulaity.sql.SQLGenerator.generateSQLRule
import org.apache.spark.sql.SparkSession

object RulesExecutor {

  def execute(dqConfiguration: DQVConfiguration, sparkSession: SparkSession, jobConfig: DQJobConfig) = {

    dqConfiguration.checkList.foreach {
      check => {
        val ruleName    =  check._1._1.asInstanceOf[String]
        val ruleValue   =  check._1._2.asInstanceOf[String]
        val description =  check._2.asInstanceOf[String]
        val sourceType  =  dqConfiguration.sourceType
        val sourcePath  =  dqConfiguration.sourcePath
        val reportType  =  dqConfiguration.reportType
        val sinkType    =  dqConfiguration.sinkType
        val sql         =  generateSQLRule(ruleName, ruleValue, sourceType, sourcePath, dqConfiguration)
        lazy val df = executeSQLRule(sql, sparkSession, sourceType, sourcePath)
        lazy val result = extractExecutionResult(df.collect().head.get(0), reportType, ruleName)
        metricGenerator(ruleName, ruleValue, result, sourceType, sourcePath, description, sinkType)
      }
    }
  }


  private def extractExecutionResult(value: Any, reportType: String, ruleName: String) = {
    if(reportType == "percentage" && ruleName != "StatisticsRule"){
      val inPercent = value.asInstanceOf[Double] * 100
      val formatToTwoDigits = BigDecimal(inPercent).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      formatToTwoDigits + "%"
    }
    else if(value.isInstanceOf[Long]) value.asInstanceOf[Long].toString
    else if(value.isInstanceOf[Double]) value.asInstanceOf[Double].toString
    else value.asInstanceOf[Int].toString
  }

}
