package vodafone.dataqulaity


object DataQualityProcessType extends Enumeration {
  type DataQualityProcessType = Value
  val profile, percentage, passedFailed = Value

  def withNameOpt(s: String): Option[Value] = values.find(_.toString == s)
}