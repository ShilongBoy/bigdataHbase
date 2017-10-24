/*
package bigdataHbase.Redis

import org.apache.spark.sql.{DataFrame, SparkSession}

object RedisConfig {

  implicit class RedisConfigBuild(builder:SparkSession.Builder){

    def buildRedis():SparkSession.Builder={
      builder.config("","")
      builder
    }

  }

  implicit class ReadJdbc(sparkSession: SparkSession) {

    def jdbcDF(table: String, numPartitions: Int = 6): DataFrame = {
      val map = Map("url" -> "", "driver" -> "", "dbtable" -> table, "numPartitions" -> s"$numPartitions")
      sparkSession.read.format("jdbc").options(map).load()
    }

  }
  }
*/
