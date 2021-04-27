package dqv.vonneumann.dataqulaity.app

import dqv.vonneumann.dataqulaity.InvalidConfigurationRule
import dqv.vonneumann.dataqulaity.config.{DQJobConfig, DQVConfigLoader, DQVConfiguration, YAMConfigLoader}
import dqv.vonneumann.dataqulaity.DataQualityProcessType
import dqv.vonneumann.dataqulaity.DataQualityProcessType.DataQualityProcessType
import dqv.vonneumann.dataqulaity.RulesExecutor.execute
import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory.createSparkSession
import io.circe.{Json, ParsingFailure}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DataQualityCheckApp {
  private val logger = LoggerFactory.getLogger(getClass)

  case class RuntimeArgument(runningMode: String, processType: DataQualityProcessType)

  def main(args: Array[String]): Unit = {
    val dqJobConfig= DQJobConfig(args)
    val runningMode = dqJobConfig.runningMode
    val sparkSession =    createSparkSession(runningMode)
    //load yaml and convert it to json structure
    YAMConfigLoader.toJson(runningMode, dqJobConfig)
                   .fold(error => reportErrors(error, dqJobConfig),
                         json  => loadRules(json, sparkSession, dqJobConfig))
  }

  private def reportErrors(error: ParsingFailure, dqJobConfig: DQJobConfig) = throw InvalidConfigurationRule (s"Please check the configuration rule structure for ${dqJobConfig.yamlPath} -> ${error.getMessage}")
  private def loadRules(json: Json, sparkSession: SparkSession, dqJobConfig: DQJobConfig) = {
    DQVConfigLoader.load(json.toString())
      .fold(
        error            => reportErrors(error, dqJobConfig),
        dqConfigurations => processDQConfiguration(dqConfigurations, sparkSession, dqJobConfig)
      )
  }

  private def processDQConfiguration(dqConfigurations: List[DQVConfiguration], sparkSession: SparkSession, dqJobConfig: DQJobConfig) = {
    dqConfigurations.foreach { dqConfiguration => {
      val sourceTypeAsString = dqConfiguration.sourceType.toString
      sourceTypeAsString match {

        case "Parquet" =>         sparkSession.
                                  read.
                                  parquet(dqConfiguration.sourcePath).
                                  createOrReplaceTempView(sourceTypeAsString)
                                  execute(dqConfiguration, sparkSession, dqJobConfig)

        case "CSV" =>             sparkSession.
                                  read.
                                  option("header", "true").
                                  option("inferSchema", "true").csv(dqConfiguration.sourcePath).
                                  createOrReplaceTempView(sourceTypeAsString)
                                  execute(dqConfiguration, sparkSession, dqJobConfig)

        case "BigQuery" =>
                                  execute(dqConfiguration, sparkSession, dqJobConfig)


        case "AWSRedshift" =>        throw new UnsupportedOperationException("***** Connector not plugged for RedShift *****")

        //Microsoft azure synapse
        case "AzureSynapse" =>    throw new UnsupportedOperationException("**** Connector not plugged for CosmosDB ****")
      }
    }
    }
  }

  private def reportErrors(error: io.circe.Error, dqJobConfig: DQJobConfig) = {
    throw InvalidConfigurationRule (s"Please check the configuration rule structure for ${dqJobConfig.yamlPath} -> ${error.getMessage}")
  }
}