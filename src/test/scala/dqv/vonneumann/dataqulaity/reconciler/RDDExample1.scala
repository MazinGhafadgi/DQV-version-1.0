package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RDDExample1 extends App {

  implicit val sparkSession = SparkSessionFactory.createSparkSession("local")
  import sparkSession.implicits._

  val OneDocRDD:RDD[String] = sparkSession.sparkContext.textFile("src/main/resources/myDoc1.txt", 2)
  val rdd = OneDocRDD.mapPartitions(iter => {
    // here you can initialize objects that you would need
    // that you want to create once by worker and not for each x in the map.
    iter.map(x => FilterGoodAndBadRow.filter(x) )
  })
  val columns = Seq("Text")
  val rightRDD:RDD[Row] = rdd.filter(_.isRight).map(x => Row(x.right.get))

  val simpleSchema = StructType(Array(
    StructField("Text",StringType,true)/*,
    StructField("middlename",StringType,true),
    StructField("lastname",StringType,true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)*/
  ))
  sparkSession.sqlContext.createDataFrame(rightRDD, simpleSchema).show(false)
}

object FilterGoodAndBadRow {
  def filter(x: String): Either[String, String] = {
    if(x.contains("Bad")) Left(x)
    else Right(x)
  }
}