package dqv.vonneumann.dataqulaity.metric

import org.slf4j.LoggerFactory
import io.circe.syntax._
import org.joda.time.DateTime
object Metric {
  private val logger = LoggerFactory.getLogger(getClass)
  private val STACKDRIVER = "stackdriver"
  private val CONSOLE = "console"

  def metricGenerator(ruleName:  String ,
                      ruleValue: String,
                      result:    String,
                      sourceType: String,
                      sourcePath: String,
                      description: String,
                      sinkType: String) = {
    val metricMap = Map("Metric-id"     -> ruleName,
      "Rule"          -> ruleValue,
      "MetricResult"  -> result,
      "SourceType"    -> sourceType,
      "SourcePath"    -> sourcePath,
      "Description"   -> description,
      "SubmissionDateTime" -> new DateTime().toString("yyyy-MM-dd HH:mm:ss"))
    sinkType match {
      case STACKDRIVER => println(metricMap.asJson.spaces4)
      case CONSOLE =>  Console.out.println(Console.GREEN_B + ruleName + Console.RESET )
                       println(metricMap.asJson.spaces4)
                       println(" \n")
      case _           => throw new RuntimeException(s"unsupported sinkType '$sinkType' only $STACKDRIVER sinkType is supported for the time being")
    }
  }

}
