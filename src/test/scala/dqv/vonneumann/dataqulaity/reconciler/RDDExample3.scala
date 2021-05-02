package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.model.ReconcilerModel
import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RDDExample3 extends App {

  implicit val sparkSession = SparkSessionFactory.createSparkSession("local")
  import sparkSession.implicits._

  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rddTest = sparkSession.sparkContext.parallelize(data)
  val dataFrame = rddTest.toDF()
  dataFrame.show(false)


  val rdd = dataFrame.rdd.mapPartitions(iter => {
    iter.map(x => FilterDFRow.filter(x) )
  })

  val badRDD  = rdd.filter(_.isLeft).map(x => x.left.get)
  val goodRDD = rdd.filter(_.isRight).map(x => x.right.get)
  val goodSchema = StructType(Array(
    StructField("goodRow",StringType,true)
  ))
  val badSchema = StructType(Array(
    StructField("badRow",StringType,true)
  ))

  val fullSchema = StructType(Array(
    StructField("Lang",StringType,true),
    StructField("Salary",StringType,true)
  ))


  sparkSession.sqlContext.createDataFrame(goodRDD, fullSchema).show(false)
  sparkSession.sqlContext.createDataFrame(badRDD, fullSchema).show(false)
}

object FilterDFRow {
  def filter(row: Row): Either[Row, Row] = {
    if(row.getString(0) == "Java") {
      Left(Row(s"BadRow ${row.getString(0)}", "No Salary"))
    } else {
      Right(row)
    }
  }
}

