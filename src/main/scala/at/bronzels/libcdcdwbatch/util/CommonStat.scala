package at.bronzels.libcdcdwbatch.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructField

object CommonStat {

  def addColumnWithDefaultValue(inputDF: DataFrame, structField: StructField, defaultValue: String = null): DataFrame = {
    val addField = structField.name
    /*if (inputDF.schema.names.map(u=> u.toLowerCase).contains(addField.toLowerCase))
      throw new Exception("The column: %s that you want add already exists".format(addField))*/
    if (defaultValue == null)
      inputDF.withColumn(addField, lit(""))
    else
      inputDF.withColumn(addField, lit(defaultValue).cast(structField.dataType))
  }

}
