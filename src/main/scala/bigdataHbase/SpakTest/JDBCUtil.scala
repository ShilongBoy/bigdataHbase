package bigdataHbase.SpakTest

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object JDBCUtil {

  private lazy val property=new Properties()
  property.load(getClass.getResourceAsStream("/mysql2mysql.properties"))
  private lazy val url=property.getProperty("url")
  private lazy val driver=property.getProperty("driver")


  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName("JDBCUtil")
    val sc=new SparkContext(conf)
    val hiveContext=new SQLContext(sc)
    import hiveContext.implicits._
    val df=Seq(("ksuy","12"),("ss","25")).toDF("name","age")
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
