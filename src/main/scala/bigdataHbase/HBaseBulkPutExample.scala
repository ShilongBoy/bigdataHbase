/*
package bigdataHbase

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.hadoop.conf.Configuration

object HBaseBulkPutExample {

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("HBaseBulkPutExample {tableName} {columnFamily}")
      return
    }
    val tableName = args(0)
    val columnFamily = args(1)
    val spark=SparkSession.builder().appName("").master("").getOrCreate()
    val  sc=spark.sparkContext


    try {
      import spark.implicits._
      val df=Seq(("1",columnFamily,"1","1")).toDF("rowKey","columnFamily","qualiter","values")
      val rs=df.map(x=>{
        val rowKey=x.getAs[String]("rowKey")
        val columnFamily=x.getAs[String]("columnFamily")
        val qualiter=x.getAs[String]("qualiter")
        val values=x.getAs[String]("values")
        val tb=hbaseTB(rowKey,columnFamily,qualiter,values)
        tb
      })

      val parmMap=Map("name"->"Otpion","SFO"->"san fan")
      val hbaseContext = new HBaseContext(sc,getConf())
      //Hbase 批量写入
      hbaseContext.bulkPut[hbaseTB](rs.rdd,
            TableName.valueOf(tableName),
            (row) => {
              val put = new Put(Bytes.toBytes(row.rowKey))
                put.addColumn(row.columnFamily.getBytes,row.qualiter.getBytes,row.values.getBytes)
              put
      })
    } finally {
      sc.stop()
    }
  }
  def getConf(): Configuration={
    val conf=HBaseConfiguration.create()
    conf.set("","")
    conf.set("","")
    conf
  }

  case class hbaseTB(rowKey:String,columnFamily:String,qualiter:String,values:String)

}
*/
