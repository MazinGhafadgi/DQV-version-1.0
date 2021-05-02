package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec



class RDDTest extends AnyFlatSpec {
  implicit val spark = SparkSessionFactory.createSparkSession("local")

  import spark.implicits._

  "convert to RDD " should "match with other dataframe " in {

    val OneDocRDD:RDD[String] = spark.sparkContext.textFile("/Users/mazinghafadgi/Documents/DQV-version-1.0/src/main/resources/myDoc1.txt", 2)
    val rdd = OneDocRDD.mapPartitions(iter => {
      // here you can initialize objects that you would need
      // that you want to create once by worker and not for each x in the map.
      iter.map(x => x + " Plus")
    })
    val columns = Seq("Text")
    val dfFromRDD2 = rdd.toDF(columns: _*)
    dfFromRDD2.show(false)
  }


  "create DataFrame from text " should "Be created" in {
    val wordsDataset = spark.sparkContext.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
    val groupedDataset = wordsDataset.flatMap(_.toLowerCase.split(" "))
      .filter(_ != "")
      .groupBy("value")
    val columns = Seq("Text", "MoreText")
    wordsDataset.map(x => Validate.validate(x)).filter(x => x.isLeft).toDF().show(false)

    val countsDataset = groupedDataset.count()
    //countsDataset.show()
  }
}

 object Validate {
   type Record = Either[String, String]
   def validate(x: String): Record = {
     if(x.contains("father"))
         Right(x)
     else Left(x)
   }
 }
