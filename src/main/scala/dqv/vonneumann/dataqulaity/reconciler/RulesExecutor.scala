package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.config.{ConfigurationContext, DQJobConfig}
import dqv.vonneumann.dataqulaity.enums.ReportType
import dqv.vonneumann.dataqulaity.metric.Metric.{metricGenerator, metricGeneratorForReconsile}
import dqv.vonneumann.dataqulaity.sql.RuleExecutor.executeSQLRule
import dqv.vonneumann.dataqulaity.sql.SQLGenerator.generateSQLRule
import dqv.vonneumann.dataqulaity.util.CountUtils
import dqv.vonneumann.dataqulaity.validation.ColumnMetaData
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}

case class RuleReport(columnName: String, count: Long, percentage: Double, ruleType: String, sourceType: String, sourcePath: String)

object RulesExecutor {

  def executeReconciler(dqConfiguration: ConfigurationContext, sparkSession: SparkSession, sourceDF: DataFrame, targetDF: DataFrame) = {
    dqConfiguration.rules.foreach {
      rule => {
        val ruleType     =  rule._1._1.asInstanceOf[String]
        val ruleValue    =  Seq(rule._1._2.asInstanceOf[String])
        val description  =  rule._2.asInstanceOf[String]
        val sourceType   =  dqConfiguration.sourceType.toString
        val sourcePath   =  dqConfiguration.sourcePath
        val sinkType     =  dqConfiguration.sinkType

        val df = Reconcile.reconcileDataFrames(sourceDF, targetDF, ruleValue, sparkSession)
        metricGeneratorForReconsile(df,ruleType, ruleValue, sourceType, sourcePath, description, sinkType)
      }
    }
  }

  def executeRules(dqConfiguration: ConfigurationContext, df: DataFrame, sparkSession:SparkSession) = {
    import sparkSession.implicits._
    import dqConfiguration._
    dqConfiguration.rules.flatMap {
      rule =>
             val ruleType =      rule._1._1.asInstanceOf[String]
             val columns    =  rule._1._2.asInstanceOf[String]
             val description  =  rule._2.asInstanceOf[String]
             val columnSeq = columns.split(",").toSeq
             val colWithFunctions: Seq[Column] = columnSeq.map {
                 column => toColFunction(column, ruleType)
             }
             val totalRows = df.count()
             val groupedDf = df.select(colWithFunctions: _*).groupBy().sum().toDF(columnSeq: _*)
            //report
              columnSeq.map {
                 columnName => {
                   val extractColumnValue = groupedDf.select(columnName).head.getLong(0)
                   val missingValue = totalRows - extractColumnValue
                   val missingValueInPercentage = CountUtils.percentage(extractColumnValue, totalRows)
                   val report = RuleReport(columnName, missingValue, missingValueInPercentage, ruleType, sourceType.toString, sourcePath)
                   Seq(report).toDS
                 }
            }
     }
  }

  def toColFunction(column: String, ruleType: String) = {
    val ruleTypeAndValue = ruleType.split("=")
    if(ruleTypeAndValue.head == "NullCheck") when(col(column).isNotNull, lit(1)).otherwise(lit(0)).alias(column)

    else if(ruleTypeAndValue.head == "InRangeCheck")  {
      val rangeList = ruleTypeAndValue.last.split(",")
      when(col(column).isInCollection(rangeList), lit(1)).otherwise(lit(0)).alias(column)
    }

    else when(col(column).isNull, lit(1)).otherwise(lit(0)).alias(column)
  }


  def selectRuleFunctions(ruleType: String, column: String) = {
    ruleType match {
      case "NullCheck" =>  when(col(column), lit(1)).otherwise(lit(0)).alias(column)
    }
  }

  private def toColumnMetaData(ruleType: String, ruleValue:String) = {
    ColumnMetaData(ruleValue, "", "any", ruleType)
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
