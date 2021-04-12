package dqv.vonneumann.dataqulaity.rules

object SQLGenerator {

  def generateSQLForStatusDataAssessment(ruleName: AnyRef, value: AnyRef, sourceName: String, sourcePath: String): String = {
    ruleName match {
      case "Validity" =>  s"SELECT CASE WHEN count(to_date($value, 'dd/mm/yyyy')) = count($value) THEN 1 ELSE 0 END FROM $sourceName"
      case _ => throw new RuntimeException(s"unsupported type of $ruleName")
    }
  }


  def generateSQLForTotalNumberOfrecord(sourceName: String): String = {
    s"SELECT count(*) FROM $sourceName"
  }

  def generateSQLForPercentage(ruleName: AnyRef, value: AnyRef, sourceName: String): String = {
    ruleName match {
      case "Validity" => s"SELECT CAST (SUM (CASE WHEN $value is NULL THEN 1 ELSE 0 END) as float) / COUNT(*) AS ProportionMissing FROM $sourceName"
      case _ => throw new RuntimeException(s"unsupported type of $ruleName")
    }
  }


  def generateSQLForPassedAndFail(ruleName: AnyRef, value: AnyRef, sourceName: String): String = {
    ruleName match {
      case "Validity" =>  s"SELECT count(to_date($value, 'dd/mm/yyyy')) from $sourceName"
      case _ => throw new RuntimeException(s"unsupported type of $ruleName")
    }
  }


  def generateSQLForProfiling(ruleName: AnyRef, value: AnyRef, sourceName: String, sourcePath: String): String = {
    ruleName match {
      case "SizeRule" => s"SELECT count(*) FROM $sourceName"
      case "MaxRule" | "MinRule" | "AVGRule" | "SumRule" | "MeanRule" => s"SELECT $value FROM $sourceName"
     // case "isComplete" => s"SELECT count(*) from $sourceName where $value"// s"SELECT CASE WHEN $filter THEN 1 ELSE 0 END FROM $sourceType"
     // case "Uniqueness" => s"SELECT CASE WHEN count(distinct $value)= count($value) THEN 1 ELSE 0 END FROM $sourceName"
      case "Completeness" => s"SELECT count(*) FROM $sourceName where $value"
      case "Uniqueness" => s"SELECT count(distinct $value) FROM $sourceName"
      case _ =>  s"SELECT count(*) FROM $sourceName where $value"
    }
  }

  def generateBigQuerySQLForProfiling(ruleName: AnyRef, value: AnyRef, sourcePath: String): String = {
    ruleName match {
      case "SizeRule" => s"SELECT count(*) FROM $sourcePath"
      case "MaxRule" | "MinRule" | "AVGRule" | "SumRule" | "MeanRule" => s"SELECT $value FROM $sourcePath"
      case "isComplete" => s"SELECT count(*) from $sourcePath where $value"// s"SELECT CASE WHEN $filter THEN 1 ELSE 0 END FROM $sourceType"
      case "Uniqueness" => s"SELECT CASE WHEN count(distinct $value)= count($value) THEN 1 ELSE 0 END FROM $sourcePath"
      case _ =>  s"SELECT count(*) FROM $sourcePath where $value"
    }
  }

}
