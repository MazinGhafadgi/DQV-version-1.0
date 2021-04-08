package vodafone.dataqulaity.config
import cats.Traverse
import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor, Json, parser}
import cats.implicits._
import com.google.cloud.storage.{BlobId, StorageOptions}

import scala.io.Source

case class ConfigRules(description:      String,
                       operationType:    String,
                       sinkType:         String,
                       sourceType:       String,
                       sourcePath:       String,
                       ruleNameAndValue: List[((String, String), String)])

object GSConfigConnector {
  /** circe is a library that creates a shapeless dependency that has an automatic deserialization function that serializes JSON string to a domain model **/
  implicit val decoder: Decoder[ConfigRules] = new Decoder[ConfigRules] {
    override def apply(hCursor: HCursor): Result[ConfigRules] =
      for {
        description             <- hCursor.downField("description").as[String]
        operationType           <- hCursor.downField("operationType").as[String]
        sinkType                <- hCursor.downField("sinkType").as[String]

        sourceList              <- hCursor.downField("sourceList").as[List[Json]]
        sourceTypes             <-  Traverse[List].traverse(sourceList)(itemJson => itemJson.hcursor.downField("source").downField("sourceType").as[String])
        sourcePaths             <-  Traverse[List].traverse(sourceList)(itemJson => itemJson.hcursor.downField("source").downField("sourcePath").as[String])

        ruleList                <- hCursor.downField("checkList").as[List[Json]]
        rileNames               <- Traverse[List].traverse(ruleList)(itemJson => itemJson.hcursor.downField("check").downField("checkType").as[String])
        ruleValues              <- Traverse[List].traverse(ruleList)(orderItemsJson => {orderItemsJson.hcursor.downField("check").downField("checkValue").as[String]})
        ruleDescription         <- Traverse[List].traverse(ruleList)(orderItemsJson => {orderItemsJson.hcursor.downField("check").downField("description").as[String]})
      } yield {
        ConfigRules(description,
                    operationType,
                    sinkType,
                    sourceTypes(0),
                    sourcePaths(0),
                    rileNames.zip(ruleValues).zip(ruleDescription))
      }
  }

  def loadConfigRules(runningInCluster: String, dqJobConfig: DQJobConfig) = {
    if(runningInCluster == "cluster") {
      import java.nio.charset.StandardCharsets.UTF_8
      val storage = StorageOptions.getDefaultInstance().getService()
      val blobId = BlobId.of(dqJobConfig.bucket, dqJobConfig.jsonFile)
      val content = storage.readAllBytes(blobId)
      val contentString = new String(content, UTF_8)
      parser.decode[List[ConfigRules]](contentString)
    }
    else{
      parser.decode[List[ConfigRules]](Source.fromFile(s"src/main/resources/${dqJobConfig.jsonFile}").mkString)
    }
  }

}