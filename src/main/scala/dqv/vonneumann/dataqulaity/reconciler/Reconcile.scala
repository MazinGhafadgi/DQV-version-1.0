package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.model.ReconcilerModel
import dqv.vonneumann.dataqulaity.util.CountUtils.percentage
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

object Reconcile {

  def reconcileDataFrames(sourceTable: DataFrame, targetTable: DataFrame, primaryKey: Seq[String],
                          spark: SparkSession):
  Dataset[ReconcilerModel] = {
    implicit val sparkSession = spark
    val sourceTableCount: Long = sourceTable.count
    val targetTableCount: Long = targetTable.count
    val sourceTableColumnsList: Seq[String] = sourceTable.columns.toSeq diff primaryKey // drop primary keys

    val targetTableWithColumnsRenamed = renameColsOnTargetTable(targetTable, primaryKey)
    val joinDataFrames = sourceTable.join(targetTableWithColumnsRenamed, primaryKey)
    val joinRecordCount = joinDataFrames.count
    val reconciledDataFrame = reconcile(sourceTableColumnsList, joinDataFrames)

    val reconcilerModelSeq: Seq[Dataset[ReconcilerModel]] = report(sourceTableColumnsList, joinRecordCount, reconciledDataFrame)

    val dataSetOfReconcilerModel:Dataset[ReconcilerModel] = reconcilerModelSeq.reduce((df1, df2) => df1.union(df2))

    enrichReport(sourceTableCount, targetTableCount, joinRecordCount, dataSetOfReconcilerModel)
  }

  //Reconcile
  private def reconcile(sourceTableColumnsList: Seq[String], joinedDataFrame: DataFrame): DataFrame = {
    val matchedColumn = sourceTableColumnsList.map { column => column + "_match" }

    val reconcilFunctions: Seq[Column] = sourceTableColumnsList.map {
      column => when(col(column) === col("target_" + column), lit(1)).otherwise(lit(0).alias(column))
    }
    val dataframeWithMatchColumns = joinedDataFrame.select(reconcilFunctions: _*).toDF(matchedColumn: _*)
    val matchingRecordCounts = dataframeWithMatchColumns.groupBy().sum().toDF(sourceTableColumnsList: _*)
    matchingRecordCounts.cache
    matchingRecordCounts
  }

  private def report(sourceTableColumnsList: Seq[String], joinRecordCount: Long, reconciledDataFrame: DataFrame)
                    (implicit spark: SparkSession): Seq[Dataset[ReconcilerModel]] = {
    import spark.implicits._
    sourceTableColumnsList.map(
      column => {
        val recordsWithSameValues: Long = reconciledDataFrame.select(col(column)).head().getLong(0)
        val recordsWithDifferentValues: Long = joinRecordCount - recordsWithSameValues
        val sameValuesPercentage: Double = percentage(recordsWithSameValues, joinRecordCount)
        val reconcilerModel = ReconcilerModel(column, recordsWithSameValues, recordsWithDifferentValues, sameValuesPercentage)
        Seq(reconcilerModel).toDS
      }
    )
  }

  private def enrichReport(sourceTableCount: Long, targetTableCount: Long, joinRecordCount: Long, dataSetOfReconcilerModel: Dataset[ReconcilerModel])
                          (implicit spark: SparkSession): Dataset[ReconcilerModel] = {
    import spark.implicits._
    dataSetOfReconcilerModel.union(
      Seq(
        ReconcilerModel("matching_record_count", joinRecordCount, sourceTableCount - joinRecordCount, percentage(joinRecordCount, sourceTableCount)),
        ReconcilerModel("dropped_records", sourceTableCount - joinRecordCount, 0, percentage(sourceTableCount - joinRecordCount, sourceTableCount)),
        ReconcilerModel("new_records", targetTableCount - joinRecordCount, 0, percentage(targetTableCount - joinRecordCount, targetTableCount))
      ).toDS)
  }

  private def renameColsOnTargetTable(targetTable: DataFrame, primaryKey: Seq[String]): DataFrame = {
    val cols = targetTable.columns.toSeq
    val renamedCols = cols.map(c => if (primaryKey.contains(c)) c else "target_" + c)
    targetTable.toDF(renamedCols: _*)
  }


}
