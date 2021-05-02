package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory

trait SparkUtil {

  implicit val sparkSession = SparkSessionFactory.createSparkSession("local")

}
