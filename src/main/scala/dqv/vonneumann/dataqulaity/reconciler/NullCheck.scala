package dqv.vonneumann.dataqulaity.reconciler

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
object NullCheck {

  def nullCheck(df: DataFrame, columns: Seq[String]): DataFrame = {
    val nullFunction :Seq[Column] =
      columns.map{colName => when(col(colName).isNull, lit(1)).otherwise(lit(0))}
    df.select(nullFunction: _*).toDF(columns: _*)
  }

}
