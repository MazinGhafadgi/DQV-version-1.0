package dqv.vonneumann.dataqulaity.validation

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
case class MetaDataResponse(isValid: Boolean , message: String)
case class ColumnMetaData(columnName: String, expectedValue: String, columnType: String, opType: String)


object ValidateDataFrameRow {
  val errorSchema = StructType(Array(
    StructField("Row",StringType,nullable = true),
    StructField("Error",StringType,nullable = true)
  ))

  def validateDataFrame(df: DataFrame, metaData: List[ColumnMetaData]) = {
    df.rdd.mapPartitions {
      itr => itr.map {
        row => validateColumns(row, metaData)
      }
    }
  }

  private def validateColumns(row: Row, columnMetaData: List[ColumnMetaData]): Either[Row, Row] = {
    val metaDataResponse:List[MetaDataResponse] =  columnMetaData.map {
      metaData => isColumnValid(metaData.opType, metaData.columnType, metaData.columnName, row, metaData.expectedValue)
    }
    val isValid = metaDataResponse.map(_.isValid).reduce((x,y) => x && y)
    if(isValid) {
      Right(row)
    } else {
      Left(row)
    }
  }

  private def isColumnValid(optType: String, colType: String, colName: String, row: Row, expectedValues: String) = {
    (colType, optType)match {
      case ("any", "NullCheck")    => if( row.getAs(colName) !=null )
                                             validMetaDataResponse else reportMe(optType, colName, s"$colName is null")
/*      case ("string","InRangeCheck")  => if( expectedValues.split(",").contains(row.getAs(colName).asInstanceOf[String]))
                                             validMetaDataResponse else reportMe(optType, colName, s"Value must be in Range of $expectedValues")*/
    }
  }


  private def reportMe(operationType: String, columnName: String, reason: String) = {
    MetaDataResponse(isValid = false, s"$columnName")
  }
  private val validMetaDataResponse = MetaDataResponse(isValid = true, "")
}