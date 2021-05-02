package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.validation.ValidateDataFrameRow.{errorSchema, validateDataFrame}
import dqv.vonneumann.dataqulaity.validation.MetaData


object RDDExample4 extends App with SparkUtil {

  val metaData = List( MetaData("gender", "", "string"), MetaData("customerID", "", "string") )

  val df = sparkSession.
    read.
    option("header", "true").
    option("inferSchema", "true").csv("src/main/resources/telco/telco-sample-customer-chum.csv")


  val validationCompleted = validateDataFrame(df, metaData)

  val badRDD1 =  validationCompleted.filter(_.isLeft).map(_.left.get)
  val goodRDD1 = validationCompleted.filter(_.isRight).map(_.right.get)

  sparkSession.sqlContext.createDataFrame(badRDD1, errorSchema).show(false)
  sparkSession.sqlContext.createDataFrame(goodRDD1, df.schema).show(false)





}
