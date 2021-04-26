package dqv.vonneumann.dataqulaity.config

import cats.Traverse
import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor, Json, parser}
import cats.implicits._
import com.google.cloud.storage.{BlobId, StorageOptions}

import scala.io.Source


case class Check(ruleName: List[String], ruleValue: List[String], descriptions: List[String])

case class DQVConfiguration(description:      String,
                            processType:      String,
                            sinkType:         String,
                            reportType:       String,
                            sourceType:       String,
                            sourcePath:       String,
                            rules: List[((String, String), String)])

object DQVConfigLoader {
  /** circe is a library that creates a shapeless dependency that has an automatic deserialization function that serializes JSON string to a domain model **/
  implicit val decoder: Decoder[DQVConfiguration] = new Decoder[DQVConfiguration] {
    override def apply(hCursor: HCursor): Result[DQVConfiguration] =
      for {
        description             <- hCursor.downField("description").as[String]
        processType           <- hCursor.downField("process.type").as[String]
        sinkType                <- hCursor.downField("sink.type").as[String]
        reportType              <- hCursor.downField("report.type").as[String]
        sourceType              <- hCursor.downField("source").downField("source.type").as[String]
        sourcePath              <- hCursor.downField("source").downField("source.path").as[String]
        ruleList                <- hCursor.downField("rules").as[List[Json]]
        rulesNames              <- Traverse[List].traverse(ruleList)(itemJson => itemJson.hcursor.downField("rule").downField("type").as[String])
        ruleValues              <- Traverse[List].traverse(ruleList)(orderItemsJson => {orderItemsJson.hcursor.downField("rule").downField("value").as[String]})
        ruleDescription         <- Traverse[List].traverse(ruleList)(orderItemsJson => {orderItemsJson.hcursor.downField("rule").downField("description").as[String]})
      } yield {
        DQVConfiguration(description,
                        processType,
                    sinkType,
                    reportType,
                    sourceType,
                    sourcePath,
                    rulesNames.zip(ruleValues).zip(ruleDescription))
      }
  }

  def load(runningInCluster: String, dqJobConfig: DQJobConfig): Either[io.circe.Error, List[DQVConfiguration]] = {
    if(runningInCluster == "cluster") {
      import java.nio.charset.StandardCharsets.UTF_8
      val storage = StorageOptions.getDefaultInstance().getService()
      val blobId = BlobId.of(dqJobConfig.bucket, dqJobConfig.jsonFile)
      val content = storage.readAllBytes(blobId)
      val contentString = new String(content, UTF_8)
      parser.decode[List[DQVConfiguration]](contentString)
    }
    else{
      parser.decode[List[DQVConfiguration]](Source.fromFile(s"src/main/resources/${dqJobConfig.jsonFile}").mkString)
    }
  }

}