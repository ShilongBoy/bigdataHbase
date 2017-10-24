package bigdataHbase.SpakTest
import java.util.Properties

import bigdataHbase.ConfigUtil
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import bigdataHbase.Test.Mysql
object Spark2Mysql {

  //val rs=ConfigUtil.readClassPathConfig[Mysql]("mysql2mysql","source")
  val url="jdbc:mysql://$host:3306/spark_test?user=root&password=123456&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=round"
  val driver="com.mysql.jdbc.Driver"

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setAppName("Spark2Mysql")
//      .setMaster("")
    val sc=new SparkContext(conf)
    val sqlcontext=new SQLContext(sc)
    import sqlcontext.implicits._
    val df=Seq(("sky","12","USA"),("sky","20","USA"),("tim","12","USA")).toDF("name","age","country")
    df.save2DB("student")
    sc.stop()
  }

  def properties(driver: String): Properties = {
    val properties = new Properties
    properties.put("driver", driver)
    //批量插入的数据大小
    properties.put("batchsize", "10000")
    properties
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

