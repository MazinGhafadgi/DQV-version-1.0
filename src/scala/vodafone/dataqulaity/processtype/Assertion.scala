package vodafone.dataqulaity.processtype

import org.apache.spark.sql.SparkSession
import vodafone.dataqulaity.DataQualityProcessType.DataQualityProcessType
import vodafone.dataqulaity.config.ConfigRules
import vodafone.dataqulaity.rules.SQLGenerator.{generateSQLForPassedAndFail, generateSQLForTotalNumberOfrecord}

object Assertion {

  def passedAndFailed(configRules: ConfigRules, sparkSession: SparkSession, dataQualityProcessType: DataQualityProcessType) = {
    println("---------------------------------------------------------------------------------------------------------------------------------------------------")
    println("%1s".format("|DQ Dimension|") + "%25s".format("|Rule|") + "%45s".format("|Passed|") +  "%45s".format("|Failed|") + "%30s".format("|SourcePath|"))
    println("---------------------------------------------------------------------------------------------------------------------------------------------------")
    configRules.ruleNameAndValue.foreach{
      ruleNameAndValue => {
        val ruleName = ruleNameAndValue._1._1
        val ruleValue = ruleNameAndValue._1._2

        ruleName match {
          case "Validity" => {
            val totalSQL = generateSQLForTotalNumberOfrecord(configRules.sourceType)
            val passedSQL = generateSQLForPassedAndFail(ruleName, ruleValue, configRules.sourceType)
            val total = sparkSession.sql(totalSQL).collect().head.get(0).asInstanceOf[Long]
            val passed = sparkSession.sql(passedSQL).collect().head.get(0).asInstanceOf[Long]
            val failed = total - passed
            val str = f"${ruleName}%-25s   ${ruleValue}%-45s   ${passed}%-15s   ${failed}%-15s   ${configRules.sourcePath}"
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
