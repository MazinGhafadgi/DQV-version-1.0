package dqv.vonneumann.dataqulaity.reconciler

case class InvalidConfigurationRule(error: String)  extends Exception(error)

