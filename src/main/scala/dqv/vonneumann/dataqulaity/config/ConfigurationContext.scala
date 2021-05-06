package dqv.vonneumann.dataqulaity.config

import cats.Traverse
import cats.implicits._
import dqv.vonneumann.dataqulaity.enums.ProcessType.ProcessType
import dqv.vonneumann.dataqulaity.enums.QualityCheckType.QualityCheckType
import dqv.vonneumann.dataqulaity.enums.{ProcessType, QualityCheckType, SinkType, SourceType, TargetType}
import dqv.vonneumann.dataqulaity.enums.SinkType.SinkType
import dqv.vonneumann.dataqulaity.enums.SourceType.SourceType
import dqv.vonneumann.dataqulaity.enums.TargetType.TargetType
import io.circe.Decoder.Result
import io.circe.JsonObject
import io.circe.{Decoder, HCursor, Json, parser}

case class Book(book: String)

case class Check(ruleName: List[String], ruleValue: List[String], descriptions: List[String])

case class Company(industry: String, year: Int, name: String, public: Boolean)

case class ConfigurationContext(
                             processType: ProcessType,
                             sinkType: SinkType,
                             qualityCheckType: QualityCheckType,
                             sourceType: SourceType,
                             sourcePath: String,
                             targetType: TargetType,
                             targetPath: String,
                             rules: Seq[((String, String), String)]
                           )

object ConfigurationContextFactory {
  implicit val decoder: Decoder[ConfigurationContext] = new Decoder[ConfigurationContext] {
    override def apply(hCursor: HCursor): Result[ConfigurationContext] = {
      val newCursor = hCursor.withFocus(json => {
        json.mapObject(jsonObject => {
          if (jsonObject.contains("target")) {
            jsonObject
          } else {
            jsonObject.add("target",
              Json.fromJsonObject(
                JsonObject.fromMap(
                  Map("target.type" -> Json.fromString("Null"),
                    "target.path" -> Json.fromString("")
                  )
                )
              )
            )
          }
        })
      })

      for {
        processType <- newCursor.downField("process.type").as[String]
        sinkType   <- newCursor.downField("sink.type").as[String]
        qualityCheckType   <- newCursor.downField("quality.check.type").as[String]
        sourceType <- newCursor.downField("source").downField("source.type").as[String]
        sourcePath <- newCursor.downField("source").downField("source.path").as[String]
        targetType <- newCursor.downField("target").downField("target.type").as[String]
        targetPath <- newCursor.downField("target").downField("target.path").as[String]
        ruleList   <- newCursor.downField("rules").as[List[Json]]
        rulesNames <- Traverse[List].traverse(ruleList)(itemJson => itemJson.hcursor.downField("rule").downField("type").as[String])
        ruleValues <- Traverse[List].traverse(ruleList)(orderItemsJson => {orderItemsJson.hcursor.downField("rule").downField("value").as[String]})
        ruleDescription <- Traverse[List].traverse(ruleList)(orderItemsJson => {orderItemsJson.hcursor.downField("rule").downField("description").as[String]})
      } yield {
        ConfigurationContext(
          ProcessType.withNameOpt(processType),
          SinkType.withNameOpt(sinkType),
          QualityCheckType.withNameOpt(qualityCheckType),
          SourceType.withNameOpt(sourceType),
          sourcePath,
          TargetType.withNameOpt(targetType),
          targetPath,
          rulesNames.zip(ruleValues).zip(ruleDescription)
        )
      }
    }
  }

  def toConfigContexts(jsonContent: String): Either[io.circe.Error, List[ConfigurationContext]] = parser.decode[List[ConfigurationContext]](jsonContent)

}