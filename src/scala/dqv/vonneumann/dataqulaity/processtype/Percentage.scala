package dqv.vonneumann.dataqulaity.processtype

import dqv.vonneumann.dataqulaity.config.ConfigRules
import org.apache.spark.sql.SparkSession
import dqv.vonneumann.dataqulaity.DataQualityProcessType.DataQualityProcessType
import dqv.vonneumann.dataqulaity.rules.SQLGenerator.{generateSQLForPercentage, generateSQLForStatusDataAssessment, generateSQLForTotalNumberOfrecord}

object Percentage {

  def percentage(configRules: ConfigRules, sparkSession: SparkSession, dataQualityProcessType: DataQualityProcessType) = {
    println("---------------------------------------------------------------------------------------------------------------------------------------------------")
    println("%1s".format("|DQ Dimension|") + "%25s".format("|Rule|") + "%45s".format("|Percentage|") +  "%30s".format("|SourcePath|"))
    println("---------------------------------------------------------------------------------------------------------------------------------------------------")
    configRules.ruleNameAndValue.foreach{
      ruleNameAndValue => {
        val ruleName = ruleNameAndValue._1._1
        val ruleValue = ruleNameAndValue._1._2

        ruleName match {

          case "Validity" => {
            val sql = generateSQLForPercentage(ruleName, ruleValue, configRules.sourceType)
            val percent = (sparkSession.sql(sql).collect().head.get(0).asInstanceOf[Double] * 100) + " %"
            val str = f"${ruleName}%-25s   ${ruleValue}%-45s   ${percent}%-15s   ${configRules.sourcePath}"
            println(str)
          }

          case _ =>  {
          //  val res = if(df.collect().head.get(0).asInstanceOf[Integer] == 1) "Success" else "Failure"
           // val str = f"${ruleName}%-25s   ${ruleValue}%-45s   $res%-15s   ${configRules.sourceName}%-15s   ${configRules.sourcePath}"
           // println(str)
          }
        }
        println("\n")
      }
    }
  }

}
