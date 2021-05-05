package dqv.vonneumann.dataqulaity.app

import dqv.vonneumann.dataqulaity.config.{ConfigurationContext, ConfigurationContextFactory, DQJobConfig, YAMConfigLoader}
import dqv.vonneumann.dataqulaity.reconciler.InvalidConfigurationRule
import dqv.vonneumann.dataqulaity.reconciler.RulesExecutor.{executeRules, executeReconciler}
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
        dqConfigurations => {
          if( dqConfigurations.map(_.targetType.toString).filter(x => x =="Null").nonEmpty)
              processDQConfiguration(dqConfigurations, sparkSession, dqJobConfig)
          else reconcile(dqConfigurations, sparkSession, dqJobConfig)

        }
      )
  }

  private def reconcile(configurationContexts: List[ConfigurationContext], sparkSession: SparkSession, dqJobConfig: DQJobConfig) = {
    configurationContexts.foreach {
      configurationContext => {
        val sourceDF = sparkSession.
          read.
          option("header", "true").
          option("inferSchema", "true").csv(configurationContext.sourcePath)

        val targetDF = sparkSession.
          read.
          option("header", "true").
          option("inferSchema", "true").csv(configurationContext.targetPath)

        executeReconciler(configurationContext, sparkSession, sourceDF, targetDF).head.show(false)
      }
    }
  }

  private def processDQConfiguration(configurationContexts: List[ConfigurationContext], sparkSession: SparkSession, dqJobConfig: DQJobConfig) = {
  val resports =  configurationContexts.map {
      configurationContext => {
      val sourceTypeAsString = configurationContext.sourceType.toString
        sourceTypeAsString match {

          case "Parquet" =>
            val df = sparkSession.read.parquet(configurationContext.sourcePath)
            val result =  executeRules(configurationContext, df, sparkSession)
            result.reduce((df1, df2) => df1.union(df2))

          case "CSV" =>
            val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(configurationContext.sourcePath)
            val result =  executeRules(configurationContext, df, sparkSession)
            result.reduce((df1, df2) => df1.union(df2))

          case "BigQuery" =>
            val df = sparkSession.read.format("bigquery").load(configurationContext.sourcePath)
            val result =  executeRules(configurationContext, df, sparkSession)
            result.reduce((df1, df2) => df1.union(df2))
        }
      }
    }
    val report = resports.reduce((ds1, ds2) => ds1.union(ds2))
    report.show(100,false)
    //report.write.format("com.databricks.spark.csv").save("report")
    //report.write.csv("src/main/resources/report")

  }

  private def reportErrors(error: io.circe.Error, dqJobConfig: DQJobConfig) = {
    throw InvalidConfigurationRule (s"Please check the configuration rule structure for ${dqJobConfig.yamlPath} -> ${error.getMessage}")
  }
}
