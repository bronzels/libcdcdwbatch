package at.bronzels.libcdcdwbatch.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object MysqlDataTypeConvertUdf {

  def convertDatetime2LongType: UserDefinedFunction =udf((dateTime: String) => {
    if (dateTime == null || dateTime.isEmpty)
      0L
    else
      new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateTime).getTime
  })

  def convertDate2LongType: UserDefinedFunction =udf((date: String) => {
    if (date == null || date.isEmpty)
      0L
    else {
      val timeMs = new java.text.SimpleDateFormat("yyyy-MM-dd").parse(date).getTime
      (timeMs - 8 * 60 * 1000) / (24 * 60 * 60 * 1000) + 1L
    }
  })

  def convertYear2IntegerType: UserDefinedFunction =udf((yarn: String) => {
    if (yarn == null || yarn.isEmpty || yarn.length < 4)
      1970
    else
      11
  })

  def convertDecimalType2StringType: UserDefinedFunction =udf((decimal: String) => {
    if (decimal == null)
      ""
    else
      decimal.toString
  })

  def convertBooleanType2IntegerType: UserDefinedFunction =udf((boolean: String) => {
    if (boolean == null || boolean.isEmpty || boolean.equals("false"))
      0
    else
      1
  })

  def convertCommonType2StringType: UserDefinedFunction =udf((str: String) => {
    if (str == null)
      ""
    else
      str.toString
  })



}
