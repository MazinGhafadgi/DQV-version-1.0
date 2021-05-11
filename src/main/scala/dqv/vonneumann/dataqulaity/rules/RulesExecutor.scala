package dqv.vonneumann.dataqulaity.rules

import dqv.vonneumann.dataqulaity.config.{ConfigurationContext, DQJobConfig}
import dqv.vonneumann.dataqulaity.enums.QualityCheckType
import dqv.vonneumann.dataqulaity.reconciler.{Reconcile, ReconcileModel}
import dqv.vonneumann.dataqulaity.report.{MetricReport, RuleReport}
import dqv.vonneumann.dataqulaity.sql.RuleExecutor.executeSQLRule
import dqv.vonneumann.dataqulaity.sql.SQLGenerator.generateSQLRule
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

object RulesExecutor {
  def execute(dqConfiguration: ConfigurationContext, inputDf: DataFrame)(implicit sparkSession: SparkSession) = {
    import dqConfiguration._
    val result = dqConfiguration.rules.map {
      rule =>
        val ruleType = rule._1._1.asInstanceOf[String]
        val columns = rule._1._2.asInstanceOf[String]
        RuleChecks.toColumnNamesAndFunctions(columns, ruleType)
    }

    val columnsAsString:Seq[String] = result.map(x => x._1).flatten
    val columnsAsColumn: Seq[Column] = result.map(x => x._2).flatten

/*
    val excludeCheckColumns = columnsAsString

    inputDf.select(columnsAsColumn ++ Seq(col("customerID"), col("TotalCharges"), col("EmailAddress"), col("Partner")): _*)
      .toDF(columnsAsString.map(c => c + "_Check") ++ Seq("customerID", "TotalCharges", "EmailAddress", "Partner"): _*).show(false)
*/


   val resultDf =  inputDf.select(columnsAsColumn ++ inputDf.columns.toSeq.map(c => col(c)): _*)
      .toDF(columnsAsString.map(c => c + "_Check") ++ inputDf.columns.toSeq: _*)

    resultDf.show(false)
    resultDf.filter("customerID_Check == 0 or TotalCharges_Check == 0 or EmailAddress_Check == 0").show(false)

  }

  def executeReconciler(dqConfiguration: ConfigurationContext, sourceDF: DataFrame, targetDF: DataFrame)(implicit sparkSession: SparkSession): Seq[Dataset[ReconcileModel]] = {
    dqConfiguration.rules.map {
      rule => {
        val ruleValue = Seq(rule._1._2.asInstanceOf[String])
        Reconcile.reconcileDataFrames(sourceDF, targetDF, ruleValue)
      }
    }
  }


  private def extractExecutionResult(value: Any, reportType: String, ruleType: String) = {
    if (reportType == QualityCheckType.Reconcile.toString && ruleType != "StatisticsRule") {
      val inPercent = value.asInstanceOf[Double] * 100
      val formatToTwoDigits = BigDecimal(inPercent).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      formatToTwoDigits + "%"
    }
    else if (value.isInstanceOf[Long]) value.asInstanceOf[Long].toString
    else if (value.isInstanceOf[Double]) value.asInstanceOf[Double].toString
    else value.asInstanceOf[Int].toString
  }


  @deprecated
  def execute(dqConfiguration: ConfigurationContext, sparkSession: SparkSession, jobConfig: DQJobConfig) = {

    dqConfiguration.rules.foreach {
      rule => {
        val ruleType = rule._1._1.asInstanceOf[String]
        val ruleValue = rule._1._2.asInstanceOf[String]
        val description = rule._2.asInstanceOf[String]
        val sourceType = dqConfiguration.sourceType.toString
        val sourcePath = dqConfiguration.sourcePath
        val reportType = "Percentage" // "Count"
        val sinkType = dqConfiguration.sinkType
        val sql = generateSQLRule(ruleType, ruleValue, sourceType, sourcePath, dqConfiguration)
        lazy val df = executeSQLRule(sql, sparkSession, sourceType, sourcePath)
        lazy val result = extractExecutionResult(df.collect().head.get(0), reportType, ruleType)
        //metricGenerator(ruleType, ruleValue, result, sourceType, sourcePath, description, sinkType)
      }
    }
  }

}
