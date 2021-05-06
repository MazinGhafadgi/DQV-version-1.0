package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.config.{ConfigurationContext, DQJobConfig}
import dqv.vonneumann.dataqulaity.enums.ReportType
import dqv.vonneumann.dataqulaity.metric.Metric.metricGenerator
import dqv.vonneumann.dataqulaity.report.{MetricReport, RuleReport}
import dqv.vonneumann.dataqulaity.rules.RuleChecks
import dqv.vonneumann.dataqulaity.sql.RuleExecutor.executeSQLRule
import dqv.vonneumann.dataqulaity.sql.SQLGenerator.generateSQLRule
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object RulesExecutor {
  def executeRules(dqConfiguration: ConfigurationContext, inputDf: DataFrame)(implicit sparkSession:SparkSession): Seq[Dataset[RuleReport]] = {
    import dqConfiguration._
    dqConfiguration.rules.flatMap {
      rule =>
             val ruleType     =     rule._1._1.asInstanceOf[String]
             val columns      =     rule._1._2.asInstanceOf[String]
             val description  =     rule._2.asInstanceOf[String]
             val (columnNames, columnFunctions) =  RuleChecks.toColumnNamesAndFunctions(columns, ruleType)
             val computedDf = inputDf.select(columnFunctions: _*).groupBy().sum().toDF(columnNames: _*)
             val metricReport = MetricReport(inputDf, computedDf)
            //report
                columnNames.map {
                 columnName => {
                   metricReport.generateReport(columnName, sourceType.toString, sourcePath, description)
                 }
            }
     }
  }

  def executeReconciler(dqConfiguration: ConfigurationContext, sparkSession: SparkSession, sourceDF: DataFrame, targetDF: DataFrame) = {
    dqConfiguration.rules.map {
      rule => {
        val ruleValue    =  Seq(rule._1._2.asInstanceOf[String])
        Reconcile.reconcileDataFrames(sourceDF, targetDF, ruleValue, sparkSession)
      }
    }
  }


  private def extractExecutionResult(value: Any, reportType: String, ruleType: String) = {
    if(reportType == ReportType.Percentage.toString && ruleType != "StatisticsRule"){
      val inPercent = value.asInstanceOf[Double] * 100
      val formatToTwoDigits = BigDecimal(inPercent).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      formatToTwoDigits + "%"
    }
    else if(value.isInstanceOf[Long]) value.asInstanceOf[Long].toString
    else if(value.isInstanceOf[Double]) value.asInstanceOf[Double].toString
    else value.asInstanceOf[Int].toString
  }


    @deprecated
    def execute(dqConfiguration: ConfigurationContext, sparkSession: SparkSession, jobConfig: DQJobConfig) = {

    dqConfiguration.rules.foreach {
      rule => {
        val ruleType =      rule._1._1.asInstanceOf[String]
        val ruleValue    =  rule._1._2.asInstanceOf[String]
        val description  =  rule._2.asInstanceOf[String]
        val sourceType   =  dqConfiguration.sourceType.toString
        val sourcePath   =  dqConfiguration.sourcePath
        val reportType   =  dqConfiguration.reportType.toString
        val sinkType     =  dqConfiguration.sinkType
        val sql          =  generateSQLRule(ruleType, ruleValue, sourceType, sourcePath, dqConfiguration)
        lazy val df      =  executeSQLRule(sql, sparkSession, sourceType, sourcePath)
        lazy val result  =  extractExecutionResult(df.collect().head.get(0), reportType, ruleType)
        metricGenerator(ruleType, ruleValue, result, sourceType, sourcePath, description, sinkType)
      }
    }
  }

}
