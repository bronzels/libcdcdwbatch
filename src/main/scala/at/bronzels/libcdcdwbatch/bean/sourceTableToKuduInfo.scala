package at.bronzels.libcdcdwbatch.bean

import scala.beans.BeanProperty

class sourceTableToKuduInfo(connUrl: String, tableName: String, outputTableName: String, primaryKeyColArr: Array[String]){

  @BeanProperty var sourceConnUrl: String = connUrl
  @BeanProperty var sourceTableName: String = tableName
  @BeanProperty var kuduOutputName: String = outputTableName
  @BeanProperty var kuduPrimaryKeyFieldArr: Array[String] = primaryKeyColArr

  def this(connUrl: String, sourceTableName: String, primaryKeyColArr: Array[String]){
    this(connUrl, sourceTableName, sourceTableName, primaryKeyColArr)
  }
}
