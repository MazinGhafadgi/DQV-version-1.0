package dqv.vonneumann.dataqulaity

case class InvalidConfigurationRule(error: String)  extends Exception(error)

