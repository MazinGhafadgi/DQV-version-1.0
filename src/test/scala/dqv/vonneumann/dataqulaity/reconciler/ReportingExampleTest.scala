package dqv.vonneumann.dataqulaity.reconciler

import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory
import org.scalatest.flatspec.AnyFlatSpec
case class Employee(employee_name: String, department: String, state: String, salary: Double, age: Int, bonus: Double)

class ReportingExampleTest extends AnyFlatSpec {
  val spark = SparkSessionFactory.createSparkSession("local")

  import spark.implicits._

  "reconcilerDataFramesOptimized - The output of this step " should "match with other dataframe " in {

    val df = Seq(
      Employee("James", "Sales", "NY", 90000, 34, 10000),
      Employee("Michael", "Sales", "NY", 86000, 56, 20000),
      Employee("Robert", "Sales", "CA", 81000, 30, 23000),
      Employee("Maria", "Finance", "CA", 90000, 24, 23000),
      Employee("Raman", "Finance", "CA", 99000, 40, 24000),
      Employee("Scott", "Finance", "NY", 83000, 36, 19000),
      Employee("Jen", "Finance", "NY", 79000, 53, 15000),
      Employee("Jeff", "Marketing", "CA", 80000, 25, 18000),
      Employee("Kumar", "Marketing", "NY", 91000, 50, 21000)
    ).toDS()

    df.show(false)
    df.groupBy("department").sum("salary").show(false)
    df.groupBy("department").min("salary")

    //GroupBy on multiple columns
    df.groupBy("department","state")
      .sum("salary","bonus")
      .show(false)

    import org.apache.spark.sql.functions._
    df.groupBy("department")
      .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus"))
      .show(false)



    df.groupBy("department")
      .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus"))
      .where(col("sum_bonus") >= 50000)
      .show(false)
  }

}
