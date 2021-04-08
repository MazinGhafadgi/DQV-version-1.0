package vodafone.dataqulaity.config

import org.joda.time.format.DateTimeFormat

case class DQJobConfig(bucket: String = "", jsonFile: String = "", reportType: String = "", runningMode: String = "",
                       column: String = "", format: String = "", startDate: String = "", endDate: String = "")


object DQJobConfig {

  /**
    *
    * @param args The arguments which is going to be run
    * @return JobConfig for the job
    */
  def apply(args: Array[String]): DQJobConfig = {
    val parser = new scopt.OptionParser[DQJobConfig]("JobConfig") {
      opt[String]('b', "bucket").required().valueName("value is required").action((x, c) => c.copy(bucket = x)).text("Setting bucket is required")
      opt[String]('j', "jsonFile").required().valueName("value is required").action((x, c) => c.copy(jsonFile = x)).text("Setting jsonFile is required")
      opt[String]('r', "reportType").required().valueName("value is required").action((x, c) => c.copy(reportType = x)).text("Setting reportType is required")
      opt[String]('m', "mode").required().valueName("value is required").action((x, c) => c.copy(runningMode = x)).text("Setting mode is required")
      opt[String]('c', "column").required().valueName("value is required").action((x, c) => c.copy(column = x)).text("Setting column is required")
      opt[String]('f', "format").required().valueName("value is required").action((x, c) => c.copy(format = x)).text("Setting format is required")
      opt[String]('s', "startDate").required().valueName("value is required").action((x, c) => c.copy(startDate = x)).text("Setting startDate is required")
      opt[String]('e', "endDate").required().valueName("value is required").action((x, c) => c.copy(endDate = x)).text("Setting endDate is required")
    }
    validateInputArguments(parser.parse(args, DQJobConfig()).getOrElse(throw new RuntimeException("JobArgs must be initialised")))
  }

  private def validateInputArguments(config: DQJobConfig)
  = {
    val targetBigQueryFormat = "yyyy-MM-dd"
    if(!validateClusterMode(config)) throw new RuntimeException("--mode value must be either local or cluster")
    if(!isJSONFile(config)) throw new RuntimeException("--jsonFile filename must be .json extension")
    val compareTwoDate = startDateMustBeLessThanEndDate(config)
    if(compareTwoDate == 0 || compareTwoDate > 0) throw new RuntimeException("startDate must be before endDate")
    val formatter = DateTimeFormat.forPattern(config.format)
    config.copy(startDate = formatter.parseDateTime(config.startDate).toString(targetBigQueryFormat))
          .copy(endDate = formatter.parseDateTime(config.endDate).toString(targetBigQueryFormat))
  }

  private def validateClusterMode(config: DQJobConfig) = config.runningMode == "cluster" || config.runningMode == "local"
  private def isJSONFile(config: DQJobConfig) = config.jsonFile.endsWith(".json")
  private def startDateMustBeLessThanEndDate(config: DQJobConfig) = {
    val formatter = DateTimeFormat.forPattern(config.format)
    formatter.parseDateTime(config.startDate).compareTo(formatter.parseDateTime(config.endDate))
  }




  def main(args: Array[String]): Unit = {
    val inputArgs = Array[String]("--bucket", "data-quality-acoe", "--jsonFile", "qaulityRules.json", "--mode", "local", "--column", "ABC", "--format", "dd/MM/yyyy",  "--startDate", "12/12/2020", "--endDate", "11/11/2021")
    val config = DQJobConfig(inputArgs)
    println(config)
  }

}
