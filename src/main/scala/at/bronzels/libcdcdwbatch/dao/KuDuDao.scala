package at.bronzels.libcdcdwbatch.dao

import at.bronzels.libcdcdwbatch.util.MySparkUtil
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

object KuDuDao {

  def saveFormatDF2Kudu(ss: SparkSession, kuduMasterUrl: String, afterFormatDF: DataFrame, outputTableName: String, tablePrimaryKeys: Array[String]): Unit = {
    val kuduContext:KuduContext = new KuduContext(kuduMasterUrl, ss.sparkContext)
    createTable(kuduContext, outputTableName, afterFormatDF.schema, tablePrimaryKeys)
    insertKuduTable(kuduContext, afterFormatDF, outputTableName)
  }

  def insertKuduTable(kuduContext: KuduContext, inputDF: DataFrame, kuduTableName: String) : Unit = {
    val columnLowerCaseDF = MySparkUtil.convertColumnName2Lowercase(inputDF)
    val repartitionF = columnLowerCaseDF.repartition(120)
    kuduContext.upsertRows(repartitionF, kuduTableName)
  }

  def createTable(ss: SparkSession, kuduMasterUrl: String, tableName: String, tableSchema: StructType, tablePksArr: Array[String]): Unit = {
    val kuduContext:KuduContext = new KuduContext(kuduMasterUrl, ss.sparkContext)
    createTable(kuduContext, tableName, tableSchema, tablePksArr)
  }

  def createTable(kuduContext: KuduContext, tableName: String, tableSchema: StructType, tablePksArr: Array[String], isOverride: Boolean = true): Unit = {
    val formatSchema = formatSchema4PKColumnNotNull(tableSchema, tablePksArr)
    val lowerCaseSchema = MySparkUtil.formatSchemaColumnLowercase(formatSchema)
    if(kuduContext.tableExists(tableName) && isOverride)
      dropTable(kuduContext, tableName)
    if(!kuduContext.tableExists(tableName))
      kuduContext.createTable(tableName, lowerCaseSchema, tablePksArr,
        new CreateTableOptions()
          .setNumReplicas(3)
          .addHashPartitions(tablePksArr.toSeq.map(_.toLowerCase).asJava, 15))
  }

  def formatDataFrame4PKColumnNotNull(inputDF: DataFrame, fields: Array[String], nullable: Boolean) : DataFrame = {
    val newSchema = formatSchema4PKColumnNotNull(inputDF.schema, fields)
    inputDF.sqlContext.createDataFrame(inputDF.rdd, newSchema )
  }

  def formatSchema4PKColumnNotNull(tableSchema: StructType, tablePksArr: Array[String]): StructType = {
    val lowerTablePksArr = tablePksArr.map(u => u.toLowerCase)
    val newSchema = StructType(tableSchema.map {
      case StructField(name, dataType, _, metadata) if lowerTablePksArr.contains(name.toLowerCase()) => StructField(name.toLowerCase, dataType, nullable = false, metadata)
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
