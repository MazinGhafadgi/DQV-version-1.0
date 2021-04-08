package vodafone.dataqulaity.app

import org.apache.spark.sql.SparkSession
import vodafone.dataqulaity.{DataQualityProcessType, InvalidConfigurationRule}
import vodafone.dataqulaity.DataQualityProcessType.DataQualityProcessType
import vodafone.dataqulaity.config.{ConfigRules, DQJobConfig, GSConfigConnector}
import vodafone.dataqulaity.processtype.Percentage.percentage
import vodafone.dataqulaity.processtype.Profiling.profile
import vodafone.dataqulaity.sparksession.SparkSessionFactory.createSparkSession
import org.slf4j.LoggerFactory

object DataQualityCheckApp {
  private val logger = LoggerFactory.getLogger(getClass)

  case class RuntimeArgument(runningMode: String, processType: DataQualityProcessType)

  def main(args: Array[String]): Unit = {
    val dqJobConfig= DQJobConfig(args)
    println(dqJobConfig)
    //type of report
    val typeOfReport =    DataQualityProcessType.withName(dqJobConfig.reportType)
    val runtimeArgument = RuntimeArgument(dqJobConfig.runningMode, typeOfReport)
    val sparkSession =    createSparkSession(runtimeArgument.runningMode)

    //load the rules.
    GSConfigConnector.loadConfigRules(runtimeArgument.runningMode, dqJobConfig) match {

      case Right(ruleConfig) => {

        ruleConfig.foreach { configRule => {
          val sourceName = configRule.sourceType
          configRule.sourceType match {

            //Parquet File
            case "ParquetFile" => sparkSession
              .read
              .parquet(configRule.sourcePath)
              .createOrReplaceTempView(configRule.sourceType)
              loadTypeOfReport(typeOfReport, configRule, sparkSession, dqJobConfig)

            //CSV File
            case "CSVFile" => sparkSession
              .read
              .option("header", "true")
              .option("inferSchema", "true").csv(configRule.sourcePath)
              .createOrReplaceTempView(configRule.sourceType)
              loadTypeOfReport(typeOfReport, configRule, sparkSession, dqJobConfig)

            //GCP BigQuery
            case "BigQuery" =>
              loadTypeOfReport(typeOfReport, configRule, sparkSession, dqJobConfig)


            //AWS Redshift
            case "Redshift " => throw new UnsupportedOperationException("***** Connector not plugged for RedShift *****")

            //Microsoft Azure Cosmos DB
            case "CosmosDB" => throw new UnsupportedOperationException("**** Connector not plugged for CosmosDB ****")
          }
        }
        }
      }
      case Left(ex) => throw InvalidConfigurationRule (s"Please check the configuration rule structure for ${dqJobConfig.jsonFile} -> ${ex.getMessage}")
    }
  }

  def loadTypeOfReport(reportType: DataQualityProcessType, configRules: ConfigRules, sparkSession: SparkSession, jobConfig: DQJobConfig) = reportType match {
    case DataQualityProcessType.profile => profile(configRules, sparkSession, jobConfig)
    case DataQualityProcessType.percentage => percentage(configRules, sparkSession, reportType)
  }

}