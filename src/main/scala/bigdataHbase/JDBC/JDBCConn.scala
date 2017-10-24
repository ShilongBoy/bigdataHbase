/*
package bigdataHbase.JDBC

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


private[bigdataHbase] trait JDBCConn{

  def url:String = ???

  def passowrd:String= ???

  def driver:String = ???

  def properties(driver: String): Properties = {
    val properties = new Properties
    properties.put("driver", driver)
    //批量插入的数据大小
    properties.put("batchsize", "10000")
    properties
  }


  implicit class ReadJDBC( sparkSession: SparkSession){

    def jdbcDF(tName:String,numParation:String):DataFrame= {
      val map = Map("url" -> url, "driver" -> driver)
      sparkSession.read.format("jdbc").options(map).load()
    }

  }

  implicit class DataFrame2JDBC( df: DataFrame){


    def save2DB(tName:String,numParation:Option[Int]=None)={
      numParation match {
        case None=>  df.write.mode(SaveMode.Append).jdbc(tName,url,properties(driver))
        case Some(num)=>df.coalesce(num).write.mode(SaveMode.Append).jdbc(tName,url,properties(driver))
      }
    }
  }
}


private[bigdataHbase] object MysqlConn {

  def build(configName:String,rootName:String):MysqlConn=new MysqlConn(configName,rootName)
}

case class MysqlConn(configName:String,rootName:String) extends JDBCConn{


}*/
