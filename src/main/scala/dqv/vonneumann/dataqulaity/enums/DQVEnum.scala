package dqv.vonneumann.dataqulaity.enums

object ReportType extends Enumeration {
    type ReportType = Value
    val Percentage, Count = Value
    def withNameOpt(s: String): Value = values.find(_.toString == s).getOrElse(throw new RuntimeException(s"Incorrect Type $s, supported ReportType (Percentage, Count)"))
  }

object SourceType extends Enumeration {
  type SourceType = Value
  val CSV, Parquet, BigQuery = Value
  def withNameOpt(st: String): Value = values.find(_.toString == st).getOrElse(throw new RuntimeException(s"Incorrect Type $st, supported SourceType (CSV, Parquet, BigQuery)"))
}

object TargetType extends Enumeration {
  type TargetType = Value
  val CSV, Parquet, BigQuery, Null = Value
  def withNameOpt(st: String): Value = values.find(_.toString == st).getOrElse(throw new RuntimeException(s"Incorrect Type $st, supported TargetType (CSV, Parquet, BigQuery)"))
}

object ProcessType extends Enumeration {
  type ProcessType = Value
  val Batch, Streaming = Value
  def withNameOpt(pt: String): Value = values.find(_.toString == pt).getOrElse(throw new RuntimeException(s"Incorrect Type $pt, supported ProcessType (Batch, Streaming)"))
}

object SinkType extends Enumeration {
  type SinkType = Value
  val Console, BigQuery = Value
  def withNameOpt(pt: String): Value = values.find(_.toString == pt).getOrElse(throw new RuntimeException(s"Incorrect Type $pt, supported SinkType (Console, BigQuery)"))
}

  object MainApp {
  def main(args: Array[String]): Unit = {
    println( ReportType.withNameOpt("XYZ").toString)
  }

}


