package vodafone.dataqulaity.output

import io.bretty.console.table.{Alignment, ColumnFormatter, Table}
import org.slf4j.LoggerFactory
import io.circe.syntax._

object Metric {
  private val logger = LoggerFactory.getLogger(getClass)
  private val STACKDRIVER = "stackdriver"
  private val CONSOLE = "console"

  def metric(metricMap: Map[String, String], sinkType: String) = {
    Console.out.println( "Test " + Console.YELLOW_B + " RED " + Console.RESET )
    sinkType match {
      case STACKDRIVER => println(metricMap.asJson.spaces4)
      case CONSOLE => println(metricMap.asJson.spaces4)
      case _           => throw new RuntimeException(s"unsupported sinkType '$sinkType' only $STACKDRIVER sinkType is supported for the time being")
    }
  }


  def metricInTableFormat(columns: Array[String], values: Array[String]) = {
    val columnsFormatter = ColumnFormatter.text(Alignment.LEFT, 20)
    val valuesFormatter = ColumnFormatter.text(Alignment.LEFT, 70)
    val builder = new Table.Builder("Key", columns, columnsFormatter)
    builder.addColumn("Value", values, valuesFormatter)
    val table = builder.build()
    println(table)
  }

}
