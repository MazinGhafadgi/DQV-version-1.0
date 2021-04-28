package dqv.vonneumann.dataqulaity.config

import cats.Traverse
import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor, Json, parser}
import cats.implicits._
import dqv.vonneumann.dataqulaity.enums.{ProcessType, ReportType, SourceType, SinkType}
import dqv.vonneumann.dataqulaity.enums.ReportType.ReportType
import dqv.vonneumann.dataqulaity.enums.SourceType.SourceType
import dqv.vonneumann.dataqulaity.enums.ProcessType.ProcessType
import dqv.vonneumann.dataqulaity.enums.SinkType.SinkType
case class Check(ruleName: List[String], ruleValue: List[String], descriptions: List[String])

case class DQVConfiguration(
                            processType:      ProcessType,
                            sinkType:         SinkType,
                            reportType:       ReportType,
                            sourceType:       SourceType,
                            sourcePath:       String,
                            rules: List[((String, String), String)]
                           )

object DQVConfigLoader {
  implicit val decoder: Decoder[DQVConfiguration] = new Decoder[DQVConfiguration] {
    override def apply(hCursor: HCursor): Result[DQVConfiguration] =
      for {
        processType             <- hCursor.downField("process.type").as[String]
        sinkType                <- hCursor.downField("sink.type").as[String]
        reportType              <- hCursor.downField("report.type").as[String]
        sourceType              <- hCursor.downField("source").downField("source.type").as[String]
        sourcePath              <- hCursor.downField("source").downField("source.path").as[String]
        ruleList                <- hCursor.downField("rules").as[List[Json]]
        rulesNames              <- Traverse[List].traverse(ruleList)(itemJson => itemJson.hcursor.downField("rule").downField("type").as[String])
        ruleValues              <- Traverse[List].traverse(ruleList)(orderItemsJson => {orderItemsJson.hcursor.downField("rule").downField("value").as[String]})
        ruleDescription         <- Traverse[List].traverse(ruleList)(orderItemsJson => {orderItemsJson.hcursor.downField("rule").downField("description").as[String]})
      } yield {
        DQVConfiguration(
                          ProcessType.withNameOpt(processType),
                          SinkType.withNameOpt(sinkType),
                          ReportType.withNameOpt(reportType),
                          SourceType.withNameOpt(sourceType),
                          sourcePath,
                          rulesNames.zip(ruleValues).zip(ruleDescription)
                        )
      }
  }

  def load(jsonContent: String): Either[io.circe.Error, List[DQVConfiguration]] = parser.decode[List[DQVConfiguration]](jsonContent)

}