package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}


case class MetaData(columnName: String, expectedValue: String, typeColumn: String)

object RDDExample3 extends App {

  val metaData = List(MetaData("language", "Java", "string"), MetaData("users_count", "3000", "int"))

  implicit val sparkSession = SparkSessionFactory.createSparkSession("local")
  import sparkSession.implicits._

  val columns = Seq("language","users_count")
  val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rddTest = sparkSession.sparkContext.parallelize(data)
  val dataFrame = rddTest.toDF(columns: _*)

  def filterWithTargetColumn(targetColumn: String, targetValue: Int ) = {
    dataFrame.rdd.mapPartitions {
      itr => itr.map {
        row => FilterDFRow.filterWithTargetColumn(row, targetColumn, targetValue)
      }
    }
  }

  val filteredRDD = filterWithTargetColumn("users_count", 3000)

  val badRDD1 = filteredRDD.filter(_.isLeft).map(_.left.get)
  val goodRDD1 = filteredRDD.filter(_.isRight).map(_.right.get)

  val errorSchema = StructType(Array(
    StructField("Row",StringType,true),
    StructField("error",StringType,true)
  ))

  sparkSession.sqlContext.createDataFrame(badRDD1, errorSchema).show(false)
  sparkSession.sqlContext.createDataFrame(goodRDD1, dataFrame.schema).show(false)

}

object FilterDFRow {
  def filterWithTargetColumn(row: Row, column: String, expectedValue: Int): Either[Row, Row] = {
    val colName = row.schema.map(_.name).find(e => e == column).get
    val extractValue = row.getAs(colName).asInstanceOf[Int]
    if(extractValue > expectedValue)
      Left(Row(row.toString(), "Failed validation due to salary is less than 3000"))
    else
      Right(row)
  }
}

