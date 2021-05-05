package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.config.{ConfigurationContext, DQJobConfig}
import dqv.vonneumann.dataqulaity.enums.ReportType
import dqv.vonneumann.dataqulaity.metric.Metric.{metricGenerator, metricGeneratorForReconsile}
import dqv.vonneumann.dataqulaity.sql.RuleExecutor.executeSQLRule
import dqv.vonneumann.dataqulaity.sql.SQLGenerator.generateSQLRule
import dqv.vonneumann.dataqulaity.util.CountUtils
import dqv.vonneumann.dataqulaity.validation.ColumnMetaData
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{approx_count_distinct, col, lit, not, when}
import org.joda.time.DateTime



case class RuleReport(ColumnName: String, ErrorType: String, Count: Long, Percentage: Double,
                      SourceType: String, SourcePath: String, SubmissionDateTime: String)

object RulesExecutor {


  def executeRules(dqConfiguration: ConfigurationContext, df: DataFrame, sparkSession:SparkSession) = {
    import sparkSession.implicits._
    import dqConfiguration._
    dqConfiguration.rules.flatMap {
      rule =>
             val ruleType     =     rule._1._1.asInstanceOf[String]
             val columns      =     rule._1._2.asInstanceOf[String]
             val description  =     rule._2.asInstanceOf[String]
             val columnSeq    =     if(ruleType != "InRangeCheck")columns.split(",").toSeq else columns.split(";").toSeq
             val colWithFunctions: Seq[Column] = columnSeq.map {
                 column => toColFunction(column, ruleType)
             }
             val totalRows = df.count()
             val groupedDf = df.select(colWithFunctions: _*).groupBy().sum().toDF(columnSeq: _*)
            //report
              columnSeq.map {
                 columnName => {
                   val submittedDateTime = new DateTime().toString("yyyy-MM-dd HH:mm:ss").toString
                   val extractColumnValue = groupedDf.select(columnName).head.getLong(0)
                   val missingValue = totalRows - extractColumnValue
                   val missingValueInPercentage = CountUtils.percentage(extractColumnValue, totalRows)
                   val report = RuleReport(columnName.split("=").head, description, missingValue, missingValueInPercentage,
                                           sourceType.toString, sourcePath, submittedDateTime)
                   Seq(report).toDS
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

  def toColFunction(column: String, ruleType: String) = {
    if(ruleType == "NullCheck") when(col(column).isNotNull, lit(1)).otherwise(lit(0)).alias(column)

    else if(ruleType == "InRangeCheck")  {
      val toColumnAndRanges = column.split("=")
      val columnValue = toColumnAndRanges.head
      val rangeList = toColumnAndRanges.last.split(",")
      when(col(columnValue).isInCollection(rangeList), lit(1)).otherwise(lit(0)).alias(columnValue)
    }

    else if(ruleType == "NonNegative") when(col(column) >=0, lit(1)).otherwise(lit(0)).alias(column)

    else if(ruleType == "Uniqueness") approx_count_distinct(col(column))


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
