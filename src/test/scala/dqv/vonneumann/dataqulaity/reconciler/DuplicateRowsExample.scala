package dqv.vonneumann.dataqulaity.reconciler

object DuplicateRowsExample extends App with SparkUtil {
  import sparkSession.implicits._
  val acctDF = List(("1", "Acc1"), ("1", "Acc1"), ("1", "Acc1"), ("2", "Acc2"), ("2", "Acc2"), ("3", "Acc3"))
               .toDF("AcctId", "Details")

  acctDF.show(false)

  val countsDF = acctDF.rdd.map(rec => (rec(0), 1)).reduceByKey(_+_).map(rec=> (rec._1.toString, rec._2)).toDF("AcctId", "AcctCount")
  countsDF.show(false)
}
