package dqv.vonneumann.dataqulaity.reconciler

import org.apache.spark.sql.functions.{col, regexp_replace}
//https://mrpowers.medium.com/performing-operations-on-multiple-columns-in-a-spark-dataframe-with-foldleft-42133f82de04
object Example5 extends App with SparkUtil {
  import sparkSession.implicits._

  val sourceDF = Seq(
    ("  p a   b l o", "Paraguay"),
    ("Neymar", "B r    asil")
  ).toDF("name", "country")

  val actualDF = Seq(
    "name",
    "country"
  ).foldLeft(sourceDF) { (memoDF, colName) =>
    memoDF.withColumn(
      colName,
      regexp_replace(col(colName), "\\s+", "")
    )
  }

  actualDF.show()
}
