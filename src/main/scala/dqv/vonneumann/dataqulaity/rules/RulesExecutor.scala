package dqv.vonneumann.dataqulaity.rules

import dqv.vonneumann.dataqulaity.config.{ConfigurationContext, DQJobConfig}
import dqv.vonneumann.dataqulaity.enums.{QualityCheckType, SinkType}
import dqv.vonneumann.dataqulaity.reconciler.{Reconcile, ReconcileModel}
import dqv.vonneumann.dataqulaity.sql.RuleExecutor.executeSQLRule
import dqv.vonneumann.dataqulaity.sql.SQLGenerator.generateSQLRule
import dqv.vonneumann.dataqulaity.util.CountUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode, SparkSession}
import org.joda.time.DateTime

import java.util.UUID

case class ExecutionResult(UUID: String, SourceType: String, SourcePath: String, SubmissionDateTime: String, FailedRows: Long, TotalRowsProcessed: Long, PassPercentage: Double)

object RulesExecutor {
  def execute(dqConfiguration: ConfigurationContext, inputDf: DataFrame)(implicit sparkSession: SparkSession) = {
    import dqConfiguration._
    val result = dqConfiguration.rules.map {
      rule =>
        val ruleType = rule._1._1.asInstanceOf[String]
        val columns = rule._1._2.asInstanceOf[String]
        RuleChecks.toColumnNamesAndFunctions(columns, ruleType)
    }

    val checkedColumns:Seq[String] = result.map(x => x._1).flatten
    val columnsAsColumn: Seq[Column] = result.map(x => x._2).flatten

    val allColumnAsCol:Seq[Column] = columnsAsColumn ++ inputDf.columns.toSeq.map(c => col(c))
    val allColumnAsString: Seq[String] = checkedColumns ++ inputDf.columns.toSeq
    val conditionExpr = if(checkedColumns.size > 1 ) checkedColumns.map(column => s"$column == 1").mkString(" or ")
                        else checkedColumns.map(column => s"$column == 1").mkString

   val resultDf =  inputDf.select(allColumnAsCol: _*).toDF(allColumnAsString: _*)

    val filterDf = resultDf.filter(conditionExpr)

   val totalErrors =  checkedColumns.map(c => col(c)).reduce((a,b) => a + b)
    val filterDfWithMoreColumn =  filterDf.columns.toSeq.map(c => col(c)) ++ Seq(totalErrors)

    val filterDfWithErrorMetric = filterDf.columns.toSeq ++ Seq("ColumnErrorCount")

   val UUIDValue = UUID.randomUUID().toString
   val errorReport =  filterDf.select(filterDfWithMoreColumn: _*).toDF(filterDfWithErrorMetric: _*)
   val errorRowsWithPK = errorReport.withColumn("UUID", lit(UUIDValue))

    val aggregateColumnsAsCol = checkedColumns.map(c => col(c))
    val errorSumColumns = errorRowsWithPK.select(aggregateColumnsAsCol : _*).groupBy().sum().toDF(checkedColumns: _*)

    errorRowsWithPK.show(false)
    if(dqConfiguration.sinkType == SinkType.BigQuery )errorRowsWithPK.write.format("bigquery").option("temporaryGcsBucket","test-dqv-check").mode(SaveMode.Overwrite).save("dqvdataset.ErrorRows")

    val totalNumberOfRows = inputDf.count()
    val errorRows = errorRowsWithPK.count()
    val passes = totalNumberOfRows - errorRows
    val passPercentage = CountUtils.percentage(passes, totalNumberOfRows)

    import sparkSession.implicits._
    val reportResultDF = Seq(ExecutionResult(UUIDValue, dqConfiguration.sourceType.toString, dqConfiguration.sourcePath, new DateTime().toString("yyyy-MM-dd HH:mm:ss"), errorRows, passes, passPercentage)).toDS
    val reportResultJoinedWithErrorColumns =  reportResultDF.join(errorSumColumns.withColumn("UUID", lit(UUIDValue)), Seq("UUID"))
    reportResultJoinedWithErrorColumns.show(false)

    if(dqConfiguration.sinkType == SinkType.BigQuery ) reportResultJoinedWithErrorColumns.write.format("bigquery").option("temporaryGcsBucket","test-dqv-check").mode(SaveMode.Overwrite).save("dqvdataset.ExecutionResult")
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
