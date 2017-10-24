package bigdataHbase.SpakTest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SparkRank {

  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName("SparkRank")
    val sc=new SparkContext(conf)
    val sqlcontext=new SQLContext(sc)
    import sqlcontext.implicits._
    val df=Seq(("",()),("",())).toDF("name","age","country")




  }

}
