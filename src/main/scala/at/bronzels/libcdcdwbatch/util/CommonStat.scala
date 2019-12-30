package at.bronzels.libcdcdwbatch.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}

object CommonStat {

  case class StructFieldWHDefaultValue(structField: StructField, defaultValue: String)

  def addColumnWithDefaultValue(inputDF: DataFrame, structFieldWHDefaultValueArr: Array[StructFieldWHDefaultValue]): DataFrame = {
    var ret = inputDF
    structFieldWHDefaultValueArr.foreach(u => {
      ret = addColumnWithDefaultValue(ret, u.structField, u.defaultValue )
    })
    ret
  }

  def addColumnWithDefaultValue(inputDF: DataFrame, structField: StructField, defaultValue: String = null): DataFrame = {
    val addField = structField.name
    var tmpDF = inputDF
    /*if (inputDF.schema.names.map(u=> u).contains(addField))
      throw new Exception("The column: %s that you want add already exists".format(addField))*/
    if (defaultValue == null)
      tmpDF = inputDF.withColumn(addField, lit(null).cast(structField.dataType))
    else
      tmpDF = inputDF.withColumn(addField, lit(defaultValue).cast(structField.dataType))

    setNullableStateOfColumn(tmpDF, structField.name, structField.nullable)
  }

  def setNullableStateOfColumn(inputDF: DataFrame, targetField: String, _nullable: Boolean) : DataFrame = {
    val schema = inputDF.schema
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) if c.equals(targetField) => StructField( c, t, nullable = _nullable, m)
      case y: StructField => y
    })
    inputDF.sqlContext.createDataFrame(inputDF.rdd, newSchema)
  }

}
