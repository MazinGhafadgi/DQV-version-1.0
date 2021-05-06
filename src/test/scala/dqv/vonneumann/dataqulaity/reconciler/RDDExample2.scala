package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object RDDExample2 extends App {

  implicit val sparkSession = SparkSessionFactory.createSparkSession("local")
  import sparkSession.implicits._

  val expectedOutput = Seq(
    ReconcileModel("submitted_amount", 2, 0, 100.0),
    ReconcileModel("attending_npi", 1, 1, 50.0),
    ReconcileModel("rendering_npi", 2, 0, 100.0),
    ReconcileModel("pt_sex_e", 2, 0, 100.0),
    ReconcileModel("pt_female_e", 2, 0, 100.0),
    ReconcileModel("matching_record_count", 2, 0, 100.0),
    ReconcileModel("dropped_records"),
    ReconcileModel("new_records", 1, 0, 33.333),
    ReconcileModel("bad_field", 1, 0, 33.333)
  ).toDS

  val rdd = expectedOutput.rdd.mapPartitions(iter => {
    // here you can initialize objects that you would need
    // that you want to create once by worker and not for each x in the map.
    iter.map(x => FilterRow.filter(x) )
  })
  val badRDD  = rdd.filter(_.isLeft).map(x => Row(x.left.get))
  val goodRDD = rdd.filter(_.isRight).map(x => Row(x.right.get))
  val badSchema = StructType(Array(
    StructField("badRow",StringType,true)
  ))
  val goodSchema = StructType(Array(
    StructField("badRow",StringType,true)
  ))

  sparkSession.sqlContext.createDataFrame(goodRDD, goodSchema).show(false)
  sparkSession.sqlContext.createDataFrame(badRDD, badSchema).show(false)
}

object FilterRow {
  def filter(model: ReconcileModel): Either[String, String] = {
    if(model.field_name == "bad_field") Left(model.toString) else Right(model.toString)
  }
}
