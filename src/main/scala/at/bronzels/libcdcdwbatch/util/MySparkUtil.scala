package at.bronzels.libcdcdwbatch.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MySparkUtil {

  def convertColumnName2Lowercase(inputDF: DataFrame): DataFrame = {
    inputDF.toDF(inputDF.columns map(_.toLowerCase): _*)
  }

  def formatSchemaColumnLowercase(tableSchema: StructType): StructType = {
    StructType(tableSchema.map { u => StructField(u.name.toLowerCase, u.dataType, nullable = u.nullable, u.metadata)})
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val sparkConf = new SparkConf()
    val ss = SparkSession
      .builder()
      .config(sparkConf)
      .appName(args.mkString(" "))
      .enableHiveSupport()
      .getOrCreate()
    ss
  }

  def getHiveSupportLocalSparkSession(array: Array[String]): SparkSession = {
    val ss = SparkSession
      .builder()
      .appName("local_spark_application_name")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    ss
  }

  def getLocalSparkSession(array: Array[String]): SparkSession = {
    val ss = SparkSession
      .builder()
      .appName("local_spark_application_name")
      .master("local[*]")
      .getOrCreate()
    ss
  }

}
