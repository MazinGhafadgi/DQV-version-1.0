package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.model.ReconcilerModel
import dqv.vonneumann.dataqulaity.util.CountUtils.percentage
import dqv.vonneumann.dataqulaity.util.dfUtils.selectColumns
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

object Reconciler {

  def reconcileDataFrames(sourceTable: DataFrame, targetTable: DataFrame, primaryKey: Seq[String],
                          spark: SparkSession):
  Dataset[ReconcilerModel] = {

    import spark.implicits._
    val target = "target_"
    val sourceTableCount: Long = sourceTable.count
    val targetTableCount: Long = targetTable.count
    val sourceTableColumnsList: Seq[String] = sourceTable.columns.toSeq diff primaryKey // drop primary keys
    val reconcilerColumnsList: Seq[String] = sourceTableColumnsList ++ sourceTableColumnsList.map(c => target + c)

    val targetTableWithColumnsRenamed = renameColsOnTargetTable(targetTable, primaryKey)

    val joinDataFrames = sourceTable.join(targetTableWithColumnsRenamed, primaryKey).select(reconcilerColumnsList.map(c => col(c)): _*)

    val joinRecordCount = joinDataFrames.count

    //matchColumnsList
    val matchColumnsList: Seq[String] = sourceTableColumnsList.map{column => column + "_match"}

    //matchColumnFunctions
    val matchColumnFunctions: Seq[Column] = sourceTableColumnsList.map {
      column => when( col(column) === col(target + column), lit(1)).otherwise(lit(0).alias(column))
    }

    val dataframeWithMatchColumns = selectColumns(joinDataFrames, matchColumnsList, matchColumnFunctions, spark)
    dataframeWithMatchColumns.show(false)

    val matchingRecordCounts = dataframeWithMatchColumns.groupBy().sum().toDF(sourceTableColumnsList: _*)
    matchingRecordCounts.cache

    val reportDfs =   sourceTableColumnsList.map(
      column => {
        val recordsWithSameValues: Long = matchingRecordCounts.select(col(column)).head().getLong(0)
        val recordsWithDifferentValues: Long = joinRecordCount - recordsWithSameValues
        val sameValuesPercentage: Double = percentage(recordsWithSameValues, joinRecordCount)
        Seq(ReconcilerModel(column, recordsWithSameValues, recordsWithDifferentValues, sameValuesPercentage)).toDS
      }
    )

    val res = reportDfs.reduce(_ union _)

    res.union(
      Seq(
        ReconcilerModel("matching_record_count", joinRecordCount, sourceTableCount-joinRecordCount, percentage(joinRecordCount, sourceTableCount)),
        ReconcilerModel("dropped_records", sourceTableCount-joinRecordCount, 0, percentage(sourceTableCount-joinRecordCount, sourceTableCount)),
        ReconcilerModel("new_records", targetTableCount-joinRecordCount, 0, percentage(targetTableCount-joinRecordCount, targetTableCount))
      ).toDS)
  }

  private def renameColsOnTargetTable(targetTable: DataFrame, primaryKey: Seq[String]): DataFrame = {
    val cols = targetTable.columns.toSeq
    val renamedCols = cols.map(c =>  if(primaryKey.contains(c)) c else "target_" + c )
    targetTable.toDF(renamedCols: _*)
  }


}
