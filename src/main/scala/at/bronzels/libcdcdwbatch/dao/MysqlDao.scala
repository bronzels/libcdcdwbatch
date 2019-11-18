package at.bronzels.libcdcdwbatch.dao

import java.util.Properties

import at.bronzels.libcdcdwbatch.constants.MysqlConstants
import at.bronzels.libcdcdwbatch.conf.MySQLEnvConf
import at.bronzels.libcdcdwbatch.constants.MysqlConstants
import at.bronzels.libcdcdwbatch.udf.MysqlDataTypeConvertUdf
import at.bronzels.libcdcdwbatch.udf.MysqlDataTypeConvertUdf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MysqlDao {

  def convertMysqlDataType2DebeziumDataType(ss: SparkSession, inputDF: DataFrame, mySQLEnvConf: MySQLEnvConf, tableName: String) : DataFrame = {
    val mysqlTableSchemaWithConvertFunc = getMysqlTableSchemaWithDataTypeConvertFunc(ss, mySQLEnvConf, tableName)
    val inputFields = inputDF.schema.map(u => u.name)
    var tempConvertDF = inputDF
    mysqlTableSchemaWithConvertFunc.foreach(u => {
      if (inputFields.contains(u.structField.name)){
        val strictField = u.structField
        if (u.convertFunction != null){
          tempConvertDF = tempConvertDF.withColumn(strictField.name, u.convertFunction(col(strictField.name)))
        }
      }
    })
    tempConvertDF
  }

  def getMysqlTableSchema(ss: SparkSession, mysqlConfig: MySQLEnvConf, tableName: String): StructType = {
    val mysqlTableSchemaWithDataTypeConvertFunc: Array[structFieldWithConvertFunction] = getMysqlTableSchemaWithDataTypeConvertFunc(ss, mysqlConfig, tableName)
    mysqlTableSchemaWithDataTypeConvertFunc.map(u => u.structField)
    StructType(mysqlTableSchemaWithDataTypeConvertFunc.map(u => u.structField))
  }

  def getMysqlTableSchemaWithDataTypeConvertFunc(ss: SparkSession, mysqlConfig: MySQLEnvConf, tableName: String): Array[structFieldWithConvertFunction] = {
    val dbName = mysqlConfig.getDbName
    val executeQuerySql = getQueryMysqlColumnDataTypeSql(dbName, tableName)
    val queryResultDF = getMysqlQueryDataFrame(ss, mysqlConfig, executeQuerySql)
    queryResultDF.collect().map( record => {
      getMysqlTargetStructFieldWithConvertFun(record.getAs[String]("column_name"), record.getAs[String]("data_type"),
        nullable = if (record.getAs[String]("nullable").equals("YES")) true else false)
    })
  }

  case class structFieldWithConvertFunction(structField: StructField, convertFunction: UserDefinedFunction)

  def getMysqlTargetStructFieldWithConvertFun(fieldName: String, dataTypeStr: String, nullable: Boolean): structFieldWithConvertFunction = {
    dataTypeStr.toLowerCase match {
      case MysqlConstants.dataTypeVarchar => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, StringType, nullable), null)
      case MysqlConstants.dataTypeTinyint => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, IntegerType, nullable), null)
      case MysqlConstants.dataTypeText => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, StringType, nullable), null)
      case MysqlConstants.dataTypeDate => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, LongType, nullable), MysqlDataTypeConvertUdf.convertDate2LongType)
      case MysqlConstants.dataTypeSmallint => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, IntegerType, nullable), null)
      case MysqlConstants.dataTypeMediumint => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, IntegerType, nullable), null)
      case MysqlConstants.dataTypeInt => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, IntegerType, nullable), null)
      case MysqlConstants.dataTypeBigint => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, LongType, nullable), null)
      case MysqlConstants.dataTypeFloat => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, DoubleType, nullable), null)
      case MysqlConstants.dataTypeDouble => structFieldWithConvertFunction(StructField(fieldName.toLowerCase, DoubleType, nullable), null)
      case MysqlConstants.dataTypeDecimal => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), MysqlDataTypeConvertUdf.convertCommonType2StringType)
      case MysqlConstants.dataTypeDatetime => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  LongType, nullable), MysqlDataTypeConvertUdf.convertDatetime2LongType)
      case MysqlConstants.dataTypeTimestamp => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), MysqlDataTypeConvertUdf.convertCommonType2StringType)
      case MysqlConstants.dataTypeTime => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  LongType, nullable), MysqlDataTypeConvertUdf.convertDatetime2LongType)
      case MysqlConstants.dataTypeYear => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  IntegerType, nullable), MysqlDataTypeConvertUdf.convertYear2IntegerType)
      case MysqlConstants.dataTypeChar => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), null)
      case MysqlConstants.dataTypeTinyblob => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), MysqlDataTypeConvertUdf.convertCommonType2StringType)
      case MysqlConstants.dataTypeTinytext => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), null)
      case MysqlConstants.dataTypeBlob => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), MysqlDataTypeConvertUdf.convertCommonType2StringType)
      case MysqlConstants.dataTypeMediumblob => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), MysqlDataTypeConvertUdf.convertCommonType2StringType)
      case MysqlConstants.dataTypeMediumtext => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), null)
      case MysqlConstants.dataTypeLongblob => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), MysqlDataTypeConvertUdf.convertCommonType2StringType)
      case MysqlConstants.dataTypeLongtext => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), null)
      case MysqlConstants.dataTypeEnum => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), null)
      case MysqlConstants.dataTypeSet => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), null)
      case MysqlConstants.dataTypeBool => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  IntegerType, nullable), MysqlDataTypeConvertUdf.convertBooleanType2IntegerType)
      case MysqlConstants.dataTypeBoolean => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  IntegerType, nullable), MysqlDataTypeConvertUdf.convertBooleanType2IntegerType)
      case MysqlConstants.dataTypeBinary =>structFieldWithConvertFunction( StructField(fieldName.toLowerCase,  StringType, nullable), MysqlDataTypeConvertUdf.convertCommonType2StringType)
      case MysqlConstants.dataTypeVarchar => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), MysqlDataTypeConvertUdf.convertCommonType2StringType)
      case MysqlConstants.dataTypeBigint => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), MysqlDataTypeConvertUdf.convertCommonType2StringType)
      case _ => structFieldWithConvertFunction(StructField(fieldName.toLowerCase,  StringType, nullable), null)
    }
  }

  def getQueryMysqlColumnDataTypeSql(dbName: String, tableName: String): String = {
    val queryMysqlColumnDataTypeBaseSql = "SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, IS_NULLABLE AS nullable FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'"
    queryMysqlColumnDataTypeBaseSql.format(dbName, tableName)
  }

  def getMysqlHugeTblDF(ss: SparkSession, mySQLEnvConf: MySQLEnvConf, tableName: String, partitionColumn: String, numPartition: Int):DataFrame = {
    val boundQueryStr = "SELECT min(%s), max(%s) FROM %s ".format(partitionColumn, partitionColumn, tableName)
    val boundRow = getMysqlQueryDataFrame(ss, mySQLEnvConf, boundQueryStr).first()
    val lowerBound:Number = boundRow.getAs(0)
    val upperBound:Number = boundRow.getAs(1)
    val ret = ss.read.jdbc(mySQLEnvConf.getConnUri, tableName, partitionColumn, lowerBound.longValue(), upperBound.longValue(), numPartition, new Properties())
    ret
  }

  def getMysqlQueryDataFrame(ss: SparkSession, mySQLEnvConf: MySQLEnvConf, querySql: String): DataFrame = {
    val configMap = scala.collection.mutable.Map( "url" -> mySQLEnvConf.getConnUri,
    "user" -> mySQLEnvConf.getUserName,
    "password" -> mySQLEnvConf.getPassword,
    "dbtable" -> s"( $querySql ) t")
    val queryResultDF = ss.sqlContext.read.format( "jdbc" )
      .options(configMap)
      .load()
    queryResultDF
  }

}
