package at.bronzels.libcdcdwbatch.dao

import at.bronzels.libcdcdwbatch.util.MySparkUtil
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

object KuDuDao {

  def readKuduTable(ss: SparkSession, kuduMasterUrl: String, kuduTableName: String) : DataFrame = {
    ss.read.options(Map("kudu.master" -> kuduMasterUrl, "kudu.table" -> kuduTableName))
      .format("kudu")
      .load
  }

  def saveFormatDF2Kudu(ss: SparkSession, kuduMasterUrl: String, afterFormatDF: DataFrame, outputTableName: String, tablePrimaryKeys: Array[String], isSrcFieldNameWTUpperCase: Boolean=false, isOverride: Boolean=true): Unit = {
    val kuduContext:KuduContext = new KuduContext(kuduMasterUrl, ss.sparkContext)
    createTable(kuduContext, outputTableName, afterFormatDF.schema, tablePrimaryKeys, isSrcFieldNameWTUpperCase, isOverride)
    insertKuduTable(kuduContext, afterFormatDF, outputTableName)
  }

  def insertKuduTable(kuduContext: KuduContext, inputDF: DataFrame, kuduTableName: String) : Unit = {
    val columnLowerCaseDF = MySparkUtil.convertColumnName2Lowercase(inputDF)
    val repartitionF = columnLowerCaseDF.repartition(120)
    kuduContext.upsertRows(repartitionF, kuduTableName)
  }

  def createTable(ss: SparkSession, kuduMasterUrl: String, tableName: String, tableSchema: StructType, tablePksArr: Array[String], isSrcFieldNameWTUpperCase: Boolean): Unit = {
    val kuduContext:KuduContext = new KuduContext(kuduMasterUrl, ss.sparkContext)
    createTable(kuduContext, tableName, tableSchema, tablePksArr, isSrcFieldNameWTUpperCase)
  }

  def createTable(kuduContext: KuduContext, tableName: String, tableSchema: StructType, tablePksArr: Array[String], isSrcFieldNameWTUpperCase: Boolean, isOverride: Boolean = true): Unit = {
    val pkFormatSchema = formatSchema4PKColumnNotNull(tableSchema, tablePksArr)

    if(kuduContext.tableExists(tableName) && isOverride)
      dropTable(kuduContext, tableName)
    if(!kuduContext.tableExists(tableName))
      kuduContext.createTable(tableName, if (isSrcFieldNameWTUpperCase) MySparkUtil.formatSchemaColumnLowercase(pkFormatSchema) else pkFormatSchema, if (isSrcFieldNameWTUpperCase) tablePksArr.map(u=>u.toLowerCase) else tablePksArr,
        new CreateTableOptions()
          .setNumReplicas(3)
          .addHashPartitions( if (isSrcFieldNameWTUpperCase) tablePksArr.toSeq.map(_.toLowerCase).asJava else tablePksArr.toSeq.asJava, 15))
  }

  def formatDataFrame4PKColumnNotNull(inputDF: DataFrame, fields: Array[String], nullable: Boolean=false) : DataFrame = {
    val newSchema = formatSchema4PKColumnNotNull(inputDF.schema, fields)
    inputDF.sqlContext.createDataFrame(inputDF.rdd, newSchema )
  }

  def formatSchema4PKColumnNotNull(tableSchema: StructType, tablePksArr: Array[String]): StructType = {
    val newSchema = StructType(tableSchema.map {
      case StructField(name, dataType, _, metadata) if tablePksArr.contains(name) => StructField(name, dataType, nullable = false, metadata)
      case y: StructField => y
    })
    newSchema
  }
  def convertFieldName2LowerCase(inputDF: DataFrame) : DataFrame = {
    val newSchema = StructType(inputDF.schema.map {
      case StructField(name, dataType, nullable, metadata) => StructField(name.toLowerCase, dataType, nullable, metadata)
    })
    inputDF.sqlContext.createDataFrame(inputDF.rdd, newSchema )
  }

  def dropTables(kuduContext: KuduContext, tableList: Array[String]): Unit = {
    tableList.foreach(table => {
      dropTable(kuduContext, table)
    })
  }

  def dropTable(kuduContext: KuduContext, table: String): Unit = {
    if (kuduContext.tableExists(table))
      kuduContext.deleteTable(table)
  }
}
