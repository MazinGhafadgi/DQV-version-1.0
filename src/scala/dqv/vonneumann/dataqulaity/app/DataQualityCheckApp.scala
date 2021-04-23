package dqv.vonneumann.dataqulaity.app

import dqv.vonneumann.dataqulaity.InvalidConfigurationRule
import dqv.vonneumann.dataqulaity.config.{DQJobConfig, DQVConfiguration, GSConfigConnector}
import dqv.vonneumann.dataqulaity.DataQualityProcessType
import dqv.vonneumann.dataqulaity.DataQualityProcessType.DataQualityProcessType
import dqv.vonneumann.dataqulaity.RulesExecutor.execute
import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory.createSparkSession
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DataQualityCheckApp {
  private val logger = LoggerFactory.getLogger(getClass)

  case class RuntimeArgument(runningMode: String, processType: DataQualityProcessType)

  def main(args: Array[String]): Unit = {
    val dqJobConfig= DQJobConfig(args)
    println(dqJobConfig)
    val typeOfReport =    DataQualityProcessType.withName(dqJobConfig.reportType)
    val runtimeArgument = RuntimeArgument(dqJobConfig.runningMode, typeOfReport)
    val sparkSession =    createSparkSession(runtimeArgument.runningMode)

    GSConfigConnector
                    .loadConfigRules(runtimeArgument.runningMode, dqJobConfig)
                    .fold(
                           error            => reportConfigurationError(error, dqJobConfig),
                           dqConfigurations => processDQConfiguration(dqConfigurations, sparkSession, dqJobConfig)
                         )
  }

  private def processDQConfiguration(dqConfigurations: List[DQVConfiguration], sparkSession: SparkSession, dqJobConfig: DQJobConfig) = {
    dqConfigurations.foreach { dqConfiguration => {
      dqConfiguration.sourceType match {

        case "Parquet" =>         sparkSession.
                                  read.
                                  parquet(dqConfiguration.sourcePath).
                                  createOrReplaceTempView(dqConfiguration.sourceType)
                                  execute(dqConfiguration, sparkSession, dqJobConfig)

        case "CSV" =>             sparkSession.
                                  read.
                                  option("header", "true").
                                  option("inferSchema", "true").csv(dqConfiguration.sourcePath).
                                  createOrReplaceTempView(dqConfiguration.sourceType)
                                  execute(dqConfiguration, sparkSession, dqJobConfig)

        case "GCPBigQuery" =>
                                  execute(dqConfiguration, sparkSession, dqJobConfig)


        case "AWSRedshift" =>        throw new UnsupportedOperationException("***** Connector not plugged for RedShift *****")

        //Microsoft azure synapse
        case "AzureSynapse" =>    throw new UnsupportedOperationException("**** Connector not plugged for CosmosDB ****")
      }
    }
    }
  }

  private def reportConfigurationError(error: io.circe.Error, dqJobConfig: DQJobConfig) = {
    throw InvalidConfigurationRule (s"Please check the configuration rule structure for ${dqJobConfig.jsonFile} -> ${error.getMessage}")
  }
}