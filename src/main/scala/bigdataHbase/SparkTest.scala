/*
package bigdataHbase

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import com.redislabs.provider.redis._

object SparkTest {

  def main(args: Array[String]): Unit = {
    import bigdataHbase.Redis.RedisConfig._
    val spark=SparkSession.builder().appName("")
      .master("local[*]")
      .buildRedis()
      .getOrCreate()

    spark.jdbcDF("")

    import spark.implicits._
    val sc=spark.sparkContext

    val df=Seq(("sky","12","USA"),("sky","20","USA"),("tim","12","USA")).toDF("name","age","country")
    val df2=Seq(("sky","USA"),("tim","China")).toDF("name","country")
    val bd=sc.broadcast(df)

    val restultDF=bd.value.as("d1").join(df2.as("d2"),$"d1.name"===$"d2.name" ).select($"d1.name",$"d1.age",$"d2.country")

    restultDF.select(count($"name").as("count"),$"age").groupBy($"name")
    val resutlRDD=restultDF.selectExpr("name,country").map(x=>{
      val name=x.getAs[String]("name")
      val country=x.getAs[String]("country")
      (name,country)
    }).rdd

    sc.toRedisKV(resutlRDD)

    restultDF.show()

    spark.stop()

  }






}
*/
