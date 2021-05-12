package dqv.vonneumann.dataqulaity.rules

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{approx_count_distinct, col, lit, when}

import java.util.regex.Pattern

object RuleChecks {

  private val matched = lit(0)
  private val unMatched = lit(1)
  private val nullCheck: String => Column = (column: String) => when(col(column).isNotNull, matched).otherwise(lit(unMatched)).alias(column)
  private val inRangeCheck: String => Column = (column: String) => {
    val toColumnNameAndRange = column.split("=")
    val columnName = toColumnNameAndRange.head
    val range = toColumnNameAndRange.last.split(",")
    when(col(columnName).isInCollection(range), matched).otherwise(unMatched).alias(columnName)
  }
  private val negativeCheck: String => Column = (column: String) => when(col(column) >= 0, matched).otherwise(unMatched).alias(column)
  private val positiveCheck: String => Column = (column: String) => when(col(column) > 0, matched).otherwise(unMatched).alias(column)
  private val uniquenessCheck: String => Column = (column: String) => approx_count_distinct(col(column))
  private val emailCheck: String => Column = (column: String) => when(col(column).like("%_@__%.__%"), matched).otherwise(unMatched).alias(column)
  private val msisdnCheck: String => Column = (column: String) => when(col(column).rlike(Pattern.quote("/^\\+(?:[0-9] ?){6,14}[0-9]$/")), matched).otherwise(unMatched).alias(column)

  private val rules = Map(
    "NullCheck" -> nullCheck,
    "InRangeCheck" -> inRangeCheck,
    "NonNegativeCheck" -> negativeCheck,
    "PositiveCheck" -> positiveCheck,
    "UniquenessCheck" -> uniquenessCheck,
    "EmailCheck"-> emailCheck,
    "MSISDNCheck" -> msisdnCheck
  )

  private def toColFunction(column: String, ruleType: String): Column = rules(ruleType)(column)

  def toColumnNamesAndFunctions(columns: String, ruleType: String): (Seq[String], Seq[Column]) = {
    //columnNames.map(c => c.split("=").head)
    val columnNames = if(ruleType == "InRangeCheck") columns.split(";").toSeq  else columns.split(",").toSeq
    val columnFunctions = columnNames.map {
      column => toColFunction(column, ruleType)
    }
    if(ruleType == "InRangeCheck")
      (columnNames.map(c => c.split("=").head).map(column => s"${column}_${ruleType}"), columnFunctions)
    else
      (columnNames.map(column => s"${column}_${ruleType}"), columnFunctions)
  }
}
