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

  "Create DataFrame from Case classes " should " create it " in {
    case class Company(name: String, foundingYear: Int, numEmployees: Int)
    val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
    val df = spark.sparkContext.parallelize(inputSeq).toDF()

    val companyDS = df.as[Company]
    companyDS.show()
  }

  "create DataFrame from text " should "Be created" in {
    val wordsDataset = spark.sparkContext.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
    val groupedDataset = wordsDataset.flatMap(_.toLowerCase.split(" "))
      .filter(_ != "")
      .groupBy("value")
    val countsDataset = groupedDataset.count()
    countsDataset.show()
  }

}
