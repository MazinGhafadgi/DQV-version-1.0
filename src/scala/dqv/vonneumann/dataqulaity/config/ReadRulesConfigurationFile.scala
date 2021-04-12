package dqv.vonneumann.dataqulaity.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._

case class RuleNameAndValue(ruleName: String, ruleValue: String)

object ReadRulesConfigurationFile {

 // GSConfigConnector.loadFileFromGS()
  lazy val config: Config = ConfigFactory.load("configRules.conf")

  lazy val sourceType = config.getString("qaulity.check.sourceType")
  lazy val sourcePath = config.getString("qaulity.check.sourcePath")
  lazy val rules = config.getObjectList("qaulity.check.rules").asScala.map(x => x.unwrapped().asScala)

}
