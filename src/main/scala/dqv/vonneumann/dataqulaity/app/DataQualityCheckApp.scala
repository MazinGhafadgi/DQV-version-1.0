package dqv.vonneumann.dataqulaity.app

import dqv.vonneumann.dataqulaity.config.{DQJobConfig, ConfigurationContextFactory, ConfigurationContext, YAMConfigLoader}
import dqv.vonneumann.dataqulaity.reconciler.InvalidConfigurationRule
import dqv.vonneumann.dataqulaity.reconciler.RulesExecutor.{execute, executeReconciler}
import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory.createSparkSession
import io.circe.{Json, ParsingFailure}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DataQualityCheckApp {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val dqJobConfig  = DQJobConfig(args)
    val runningMode  = dqJobConfig.runningMode
    val sparkSession = createSparkSession(runningMode)
    //load yaml and convert it to json structure
    YAMConfigLoader.toJson(runningMode, dqJobConfig)
                   .fold( error => reportErrors(error, dqJobConfig),
                          json  => run(json, sparkSession, dqJobConfig)
                        )
  }

  private def reportErrors(error: ParsingFailure, dqJobConfig: DQJobConfig) = throw InvalidConfigurationRule (s"Please check the configuration rule structure for ${dqJobConfig.yamlPath} -> ${error.getMessage}")
  private def run(json: Json, sparkSession: SparkSession, dqJobConfig: DQJobConfig) = {
    ConfigurationContextFactory.toConfigContexts(json.toString())
      .fold(
        error            => reportErrors(error, dqJobConfig),
        dqConfigurations => processDQConfiguration(dqConfigurations, sparkSession, dqJobConfig)
      )
  }

  private def processDQConfiguration(configurationContexts: List[ConfigurationContext], sparkSession: SparkSession, dqJobConfig: DQJobConfig) = {
    configurationContexts.foreach { configurationContext => {
      val sourceTypeAsString = configurationContext.sourceType.toString
      val targetTypeAsString = configurationContext.targetType.toString

      //Reconsile path
      if (targetTypeAsString != "Null") {
        val sourceDF = sparkSession.
          read.
          option("header", "true").
          option("inferSchema", "true").csv(configurationContext.sourcePath)

        val targetDF = sparkSession.
          read.
          option("header", "true").
          option("inferSchema", "true").csv(configurationContext.targetPath)
        executeReconciler(configurationContext, sparkSession, sourceDF, targetDF)
      }

      // Normal Path
      else {
        sourceTypeAsString match {

          case "Parquet" => sparkSession.
            read.
            parquet(configurationContext.sourcePath).
            createOrReplaceTempView(sourceTypeAsString)
            execute(configurationContext, sparkSession, dqJobConfig)

          case "CSV" => sparkSession.
            read.
            option("header", "true").
            option("inferSchema", "true").csv(configurationContext.sourcePath).
            createOrReplaceTempView(sourceTypeAsString)
            execute(configurationContext, sparkSession, dqJobConfig)

          case "BigQuery" =>
            execute(configurationContext, sparkSession, dqJobConfig)


          case "AWSRedshift" => throw new UnsupportedOperationException("***** Connector not plugged for RedShift *****")

          //Microsoft azure synapse
          case "AzureSynapse" => throw new UnsupportedOperationException("**** Connector not plugged for CosmosDB ****")
        }
      }
    }
    }
  }

  private def reportErrors(error: io.circe.Error, dqJobConfig: DQJobConfig) = {
    throw InvalidConfigurationRule (s"Please check the configuration rule structure for ${dqJobConfig.yamlPath} -> ${error.getMessage}")
  }
}
