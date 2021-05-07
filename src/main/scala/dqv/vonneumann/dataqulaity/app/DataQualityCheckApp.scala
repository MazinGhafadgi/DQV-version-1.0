package dqv.vonneumann.dataqulaity.app

import dqv.vonneumann.dataqulaity.config.{ConfigurationContext, ConfigurationContextFactory, DQJobConfig, YAMConfigLoader}
import dqv.vonneumann.dataqulaity.enums.QualityCheckType
import dqv.vonneumann.dataqulaity.enums.QualityCheckType.QualityCheckType
import dqv.vonneumann.dataqulaity.reconciler.InvalidConfigurationRule
import dqv.vonneumann.dataqulaity.rules.RulesExecutor.{execute, executeReconciler}
import dqv.vonneumann.dataqulaity.sparksession.SparkSessionFactory.createSparkSession
import io.circe.{Json, ParsingFailure}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object DataQualityCheckApp {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val dqJobConfig  = DQJobConfig(args)
    val runningMode  = dqJobConfig.runningMode
    implicit val sparkSession: SparkSession = createSparkSession(runningMode)
    //load yaml and convert it to json structure
    YAMConfigLoader.toJson(runningMode, dqJobConfig)
                   .fold( error => reportErrors(error, dqJobConfig),
                          json  => run(json, dqJobConfig)
                        )
  }

  private def reportErrors(error: ParsingFailure, dqJobConfig: DQJobConfig) = throw InvalidConfigurationRule (s"Please check the configuration rule structure for ${dqJobConfig.yamlPath} -> ${error.getMessage}")
  private def run(json: Json, dqJobConfig: DQJobConfig)(implicit sparkSession: SparkSession) = {
    ConfigurationContextFactory.toConfigContexts(json.toString())
      .fold(
        error            => reportErrors(error, dqJobConfig),
        dqConfigurations => {
          dqConfigurations.map(_.qualityCheckType).head match {
            case QualityCheckType.QualityCheck => processDQConfiguration(dqConfigurations, dqJobConfig)
            case QualityCheckType.Reconcile =>   reconcile(dqConfigurations)
          }
        }
      )
  }

  private def processDQConfiguration(configurationContexts: List[ConfigurationContext], dqJobConfig: DQJobConfig)(implicit sparkSession: SparkSession) = {
    val resports =  configurationContexts.map {
      configurationContext => {
        val sourceTypeAsString = configurationContext.sourceType.toString
        sourceTypeAsString match {

          case "Parquet" =>
            val df = sparkSession.read.parquet(configurationContext.sourcePath)
            val result =  execute(configurationContext, df)
            result.reduce((df1, df2) => df1.union(df2))

          case "CSV" =>
            val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(configurationContext.sourcePath)
            val result =  execute(configurationContext, df)
            result.reduce((df1, df2) => df1.union(df2))

          case "BigQuery" =>
            val df = sparkSession.read.format("bigquery").load(configurationContext.sourcePath)
            val result =  execute(configurationContext, df)
            result.reduce((df1, df2) => df1.union(df2))
        }
      }
    }
    val report = resports.reduce((ds1, ds2) => ds1.union(ds2))
    report.show(100,false)
   // report.write.format("com.databricks.spark.csv").save("report")
    report.write.format("bigquery").option("temporaryGcsBucket","test-dqv-check").mode(SaveMode.Append).save("dqvdataset.DQVTable")
    //report.write.csv("src/main/resources/report")

  }

  private def reconcile(configurationContexts: List[ConfigurationContext])(implicit sparkSession: SparkSession) = {
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

        executeReconciler(configurationContext, sourceDF, targetDF).head.show(false)
      }
    }
  }


  private def reportErrors(error: io.circe.Error, dqJobConfig: DQJobConfig) = {
    throw InvalidConfigurationRule (s"Please check the configuration rule structure for ${dqJobConfig.yamlPath} -> ${error.getMessage}")
  }
}
