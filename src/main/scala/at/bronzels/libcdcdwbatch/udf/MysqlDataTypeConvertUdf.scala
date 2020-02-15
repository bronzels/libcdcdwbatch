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

  def convertDecimalType2DoubleType: UserDefinedFunction =udf((bigDecimal: java.math.BigDecimal) => {
    if (bigDecimal == null )
      0.0
    else
      bigDecimal.doubleValue()
  })

  def convertTimestamp2StringType: UserDefinedFunction =udf((timestamp: String) => {
    if (timestamp == null || timestamp.isEmpty)
      ""
    else
      new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp).toString
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
    else if (boolean.equals("true"))
      1
    else
      Integer.valueOf(boolean).toInt
  })

  def convertCommonType2StringType: UserDefinedFunction =udf((str: String) => {
    if (str == null)
      ""
    else
      str.toString
  })



}
