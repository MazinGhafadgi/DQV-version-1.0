package dqv.vonneumann.dataqulaity.report

import dqv.vonneumann.dataqulaity.util.CountUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.joda.time.DateTime

case class RuleReport(ColumnName: String, ColumnType: String, ErrorType: String, ErrorCount: Long, Percentage: Double,
                      SourceType: String, SourcePath: String, SubmissionDateTime: String)

case class MetricReport(inputDf: DataFrame, sumDf: DataFrame) {

  def generateReport(columnName: String, sourceType:String, sourcePath: String, description: String)(implicit sparkSession: SparkSession): Dataset[RuleReport] = {
    import sparkSession.implicits._
    val totalRows = inputDf.count()
    //report
        val cleanUpColumnName        = columnName.split("=").head
        val submittedDateTime        = new DateTime().toString("yyyy-MM-dd HH:mm:ss")
        val extractColumnValue       = sumDf.select(columnName).head.getLong(0)
        val columnType               = inputDf.select(cleanUpColumnName).schema.fields.map(f => f.dataType.typeName).head // get the original type
        val missingValue             = totalRows - extractColumnValue
        val missingValueInPercentage = CountUtils.percentage(extractColumnValue, totalRows)
        val report                   = RuleReport(cleanUpColumnName, columnType, description,
          missingValue, missingValueInPercentage,
          sourceType, sourcePath, submittedDateTime)
        Seq(report).toDS
  }

}
