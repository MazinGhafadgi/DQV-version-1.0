package dqv.vonneumann.dataqulaity.config

import com.google.cloud.storage.{BlobId, StorageOptions}
import io.circe.{parser, yaml}

import java.io.FileReader

object YAMConfigLoader {

  def toJson(runningInCluster: String, dqJobConfig: DQJobConfig) = {
    if(runningInCluster == "cluster") {
      import java.nio.charset.StandardCharsets.UTF_8
      val storage = StorageOptions.getDefaultInstance().getService()
      val blobId = BlobId.of(dqJobConfig.bucket, dqJobConfig.jsonFile)
      val content = storage.readAllBytes(blobId)
      val contentString = new String(content, UTF_8)
      yaml.parser.parse(contentString)
    }
    else{
      yaml.parser.parse(new FileReader("/Users/mazinghafadgi/Documents/DQV-version-1.0/src/main/resources/config.yml"))
    }

  }

}
