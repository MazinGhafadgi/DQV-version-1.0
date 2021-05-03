package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.validation.ValidateDataFrameRow.{errorSchema, validateDataFrame}
import dqv.vonneumann.dataqulaity.validation.ColumnMetaData


object RDDExample3 extends App with SparkUtil {

  val metaData = List(ColumnMetaData("language", "Java", "string", "eq"), ColumnMetaData("users_count", "3000", "int", "eq"))

  import sparkSession.implicits._

  val columns = Seq("language","users_count")
  val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rddTest = sparkSession.sparkContext.parallelize(data)
  val dataFrame = rddTest.toDF(columns: _*)


  val validationCompleted = validateDataFrame(dataFrame, metaData)

  val badRDD1 =  validationCompleted.filter(_.isLeft).map(_.left.get)
  val goodRDD1 = validationCompleted.filter(_.isRight).map(_.right.get)

  sparkSession.sqlContext.createDataFrame(badRDD1, errorSchema).show(false)
  sparkSession.sqlContext.createDataFrame(goodRDD1, dataFrame.schema).show(false)

}



