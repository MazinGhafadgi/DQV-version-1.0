package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.functions.{col, lit, when}

case class Person(name: Option[String], age: Option[Int])
class CheckingForNullValueTest extends AnyFlatSpec {
  val spark = SparkSessionFactory.createSparkSession("local")

  import spark.implicits._

  "reconcilerDataFramesOptimized - The output of this step " should "match with other dataframe " in {
    val df =  Seq(Person(Some("Alan"), Some(2)),
       Person(Some("Mazin"), Some(51)),
       Person(Some("James"), Some(10)),
       Person(None, Some(11))
     ).toDS()

    /**
     *     val reconcilFunctions: Seq[Column] = sourceTableColumnsList.map {
      column => when(col(column) === col("target_" + column), lit(1)).otherwise(lit(0).alias(column))
    }
     */
    val filterBasedOnNull = df.columns.toSeq.map {
      column => when(col(column).isNull, lit(0)).otherwise(lit(1))
    }

    df.show(false)
    val sumDf = df.select(filterBasedOnNull: _*).groupBy().sum().toDF(df.columns: _*)
    val totalCount =     df.count()
    val missingNames =   totalCount - sumDf.select(col("name")).head().getLong(0)
    val missingAge   =   totalCount - sumDf.select(col("age")).head().getLong(0)

    println(s"MissingName  $missingNames")
    println(s"missingAge  $missingAge")
  }

  }
