package dqv.vonneumann.dataqulaity.metric

import dqv.vonneumann.dataqulaity.enums.SinkType
import dqv.vonneumann.dataqulaity.enums.SinkType.SinkType
import dqv.vonneumann.dataqulaity.model.ReconcilerModel
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import io.circe.syntax._
import org.apache.spark.sql.{DataFrame, Dataset}
object Metric {
  private val logger = LoggerFactory.getLogger(getClass)
  private val STACKDRIVER = "stackdriver"
  private val CONSOLE = "console"

  def metricGeneratorForReconsile(df: Dataset[ReconcilerModel] ,
                                  ruleType: String,
                                  ruleValue: Seq[String],
                                  sourceType: String,
                                  sourcePath: String,
                                  description: String,
                                  sinkType: SinkType) = {
    df.show(false)

    val metricMap = Map("Metric-id" -> ruleType,
      "Rule" -> ruleValue.seq.mkString,
      "SourceType" -> sourceType,
      "SourcePath" -> sourcePath,
      "Description" -> description,
      "SubmissionDateTime" -> new DateTime().toString("yyyy-MM-dd HH:mm:ss"))
    sinkType match {
      case SinkType.BigQuery => throw new RuntimeException("Unsupported at the moment")
      case SinkType.Console => Console.out.println(Console.GREEN_B + ruleType + Console.RESET)
        println(metricMap.asJson.spaces4)
        println(" \n")
      case _ => throw new RuntimeException(s"unsupported sinkType '$sinkType' only $STACKDRIVER sinkType is supported for the time being")
    }

  }

  def metricGenerator(ruleName: String,
                      ruleValue: String,
                      result: String,
                      sourceType: String,
                      sourcePath: String,
                      description: String,
                      sinkType: SinkType) = {
    val metricMap = Map("Metric-id" -> ruleName,
      "Rule" -> ruleValue,
      "MetricResult" -> result,
      "SourceType" -> sourceType,
      "SourcePath" -> sourcePath,
      "Description" -> description,
      "SubmissionDateTime" -> new DateTime().toString("yyyy-MM-dd HH:mm:ss"))
    sinkType match {
      case SinkType.BigQuery => throw new RuntimeException("Unsupported at the moment")
      case SinkType.Console => Console.out.println(Console.GREEN_B + ruleName + Console.RESET)
        println(metricMap.asJson.spaces4)
        println(" \n")
      case _ => throw new RuntimeException(s"unsupported sinkType '$sinkType' only $STACKDRIVER sinkType is supported for the time being")
    }
  }

}
