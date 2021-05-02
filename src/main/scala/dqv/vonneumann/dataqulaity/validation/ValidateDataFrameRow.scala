package dqv.vonneumann.dataqulaity.validation

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class MetaDataResponse(isValid: Boolean , message: String)
case class MetaData(columnName: String, expectedValue: String, typeColumn: String)


object ValidateDataFrameRow {
  val errorSchema = StructType(Array(
    StructField("Row",StringType,true),
    StructField("Error",StringType,true)
  ))

  def validateDataFrame(df: DataFrame, metaData: List[MetaData]) = {
    df.rdd.mapPartitions {
      itr => itr.map {
        row => ValidateDataFrameRow.validate(row, metaData)
      }
    }
  }

  def validate(row: Row, metaData: List[MetaData]): Either[Row, Row] = {

    val expectedValue:List[MetaDataResponse] =  metaData.map {
      meataData => meataData.typeColumn match {
        case "string" => if (row.getAs(meataData.columnName).asInstanceOf[String] == meataData.expectedValue)
          validMetaDataResponse else MetaDataResponse(false, reportErrorMessage(row, meataData))

        case "int"    => if(row.getAs(meataData.columnName).asInstanceOf[Int] > meataData.expectedValue.toInt)
          validMetaDataResponse else MetaDataResponse(false, reportErrorMessage(row, meataData))

        case _ =>                MetaDataResponse(false, s"Incorrect column type ${meataData.typeColumn}")
      }
    }

    val isValid = expectedValue.map(_.isValid).reduce((x,y) => x && y)
    if(isValid) Right(row) else Left(Row("reason",expectedValue.filter(!_.isValid).map(_.message).mkString))
  }

  private val validMetaDataResponse = MetaDataResponse(true, "")
  private def reportErrorMessage(row: Row, metaData: MetaData): String =
    s"In $row and column ${metaData.columnName} failed validation rule actual value ${row.getAs(metaData.columnName)}  and expected value should be  ${metaData.expectedValue}"

}