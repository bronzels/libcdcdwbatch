package at.bronzels.libcdcdwbatch.dao

import at.bronzels.libcdcdwbatch.conf.MongoEnvConf
import com.mongodb.{MongoClient, MongoClientOptions, MongoClientURI}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MongoDao {

  def convertObjectID2StringType: UserDefinedFunction =udf((str: String) => {
    if (str == null)
      null //TODO
      //str.replace("[", "").replace("]", "")
    else
      str.replace("[", "").replace("]", "")
  })


  def getDataTypeConvertDF(ss: SparkSession, inputDF: DataFrame): DataFrame = {
    var tempConvertDF = inputDF
    inputDF.schema.foreach(structField => {
      val dataType = structField.dataType
      val fieldName = structField.name
      dataType match  {
        case DateType => tempConvertDF = tempConvertDF.withColumn(fieldName, col(fieldName).cast(StringType))
        case TimestampType => tempConvertDF = tempConvertDF.withColumn(fieldName, col(fieldName).cast(StringType))
        case DecimalType() => tempConvertDF = tempConvertDF.withColumn(fieldName, col(fieldName).cast(StringType))
        case StructType(_) => tempConvertDF = tempConvertDF.withColumn(fieldName, col(fieldName).cast(StringType))
        case ArrayType(_, _) => tempConvertDF = tempConvertDF.withColumn(fieldName, col(fieldName).cast(StringType))
        case NullType => tempConvertDF = tempConvertDF.withColumn(fieldName, col(fieldName).cast(StringType))
        case _ =>
      }
    })
    tempConvertDF
  }

  def getMongoDF(mongoEnvConf: MongoEnvConf, ss: SparkSession, toGetTableName: String): DataFrame = {
    val retDF = ss.sqlContext.read.format("com.mongodb.spark.sql")
      .options(
        Map(
          "spark.mongodb.input.uri" -> mongoEnvConf.getUrl,
          "spark.mongodb.auth.uri" -> mongoEnvConf.getUrl,
          "spark.mongodb.input.database" -> getMongoOutputDB(mongoEnvConf),
          "spark.mongodb.input.collection" -> toGetTableName,
          "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
          "spark.mongodb.input.partitionerOptions.partitionKey"  -> "_id",
          "spark.mongodb.input.partitionerOptions.partitionSizeMB"-> "32",
          "spark.mongodb.input.sampleSize" -> getMongoDatabaseTableCount(mongoEnvConf, toGetTableName).toString
        ))
      .load()
    retDF
  }

  def getMongoOutputDB(envConf: MongoEnvConf): String = {
    if (StringUtils.isNotBlank(envConf.getUrl))
      new MongoClientURI(envConf.getUrl).getDatabase
    else null
  }

  def getMongoDatabaseTableCount(envConf: MongoEnvConf, toGetTableName: String): Long = {
    val mongoClient = getMongoClient(envConf)
    val outputDb = mongoClient.getDB(getMongoOutputDB(envConf))
    val collection = outputDb.getCollection(toGetTableName)
    val count = collection.count
    mongoClient.close()
    count
  }

  def getMongoClient(envConf: MongoEnvConf): MongoClient = {
    val builder = new MongoClientOptions.Builder
    builder.maxConnectionIdleTime(60000)
    new MongoClient(new MongoClientURI(envConf.getUrl, builder))
  }

}
