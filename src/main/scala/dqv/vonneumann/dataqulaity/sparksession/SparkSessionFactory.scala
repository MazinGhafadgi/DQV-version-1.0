package dqv.vonneumann.dataqulaity.sparksession

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def createSparkSession(runningMode: String): SparkSession = {
    if(runningMode == "cluster") {
      val spark: SparkSession = SparkSession.builder.appName("spark-bigquery-demo").getOrCreate
     // spark.sparkContext.setLogLevel("ERROR")
      spark
    }
    else {
      val spark: SparkSession = SparkSession.builder()
        .master("local[1]")
        .appName("DataCheck")
        .config("spark.debug.maxToStringFields", "200")
        .config("spark.sql.debug.maxToStringFields", "200")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      spark
    }
  }

}
