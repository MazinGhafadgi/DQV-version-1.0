package dqv.vonneumann.dataqulaity.rules

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{approx_count_distinct, col, lit, when}

object RuleChecks {

  private val matched = lit(1)
  private val unMatched = lit(0)
  private val nullCheck: String => Column = (column: String) => when(col(column).isNotNull, matched).otherwise(lit(unMatched)).alias(column)
  private val inRangeCheck: String => Column = (column: String) => {
    val toColumnNameAndRange = column.split("=")
    val columnName = toColumnNameAndRange.head
    val range = toColumnNameAndRange.last.split(",")
    when(col(columnName).isInCollection(range), matched).otherwise(unMatched).alias(columnName)
  }
  private val negativeCheck: String => Column = (column: String) => when(col(column) >= 0, matched).otherwise(unMatched).alias(column)
  private val uniquenessCheck: String => Column = (column: String) => approx_count_distinct(col(column))

  private val rules = Map(
    "NullCheck" -> nullCheck,
    "InRangeCheck" -> inRangeCheck,
    "NonNegativeCheck" -> negativeCheck,
    "UniquenessCheck" -> uniquenessCheck
  )

  private def toColFunction(column: String, ruleType: String): Column = rules(ruleType)(column)

  def toColumnNamesAndFunctions(columns: String, ruleType: String): (Seq[String], Seq[Column]) = {
    val columnNames = if(ruleType != "InRangeCheck") columns.split(",").toSeq else columns.split(";").toSeq
    val columnFunctions = columnNames.map {
      column => toColFunction(column, ruleType)
    }
    (columnNames, columnFunctions)
  }
}
