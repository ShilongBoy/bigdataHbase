package bigdataHbase.JDBC

import java.util.Properties
import bigdataHbase.ConfigUtil
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

private[bigdataHbase] trait JdbcConnection {


  def user: String = ???

  def password: String = ???

  def driver: String = ???

  def url: String = ???


  def properties(driver: String): Properties = {
    val properties = new Properties
    properties.put("driver", driver)
    //批量插入的数据大小
    properties.put("batchsize", "10000")
    properties
  }

  implicit class ReadJdbc( sqlContext: SQLContext) {

    def jdbcDF(table: String, numPartitions: Int = 6): DataFrame = {
      val map = Map("url" -> url, "driver" -> driver, "dbtable" -> table, "numPartitions" -> s"$numPartitions")
      sqlContext.read.format("jdbc").options(map).load()
    }

  }

  implicit class DataFrame2Jdbc(df: DataFrame) {

    def save2DB(table: String, numPartitions: Option[Int] = None) = {
      //      val map = Map("url" -> url, "driver" -> driver, "dbtable" -> table, "numPartitions" -> s"$numPartitions")
      numPartitions match {
        case Some(num) => df.coalesce(num).write.mode(SaveMode.Append).jdbc(url, table, properties(driver))
        case None => df.write.mode(SaveMode.Append).jdbc(url, table, properties(driver))
      }

    }

  }

}

case class MysqlConnection(configName: String, rootNode: String) extends JdbcConnection {

  def config = JDBCConfig.readConfig(configName, rootNode)

  require(config.driver.contains("mysql"), "driver name must be mysql")

  override def user = config.user

  override def password = config.password

  override def driver = config.driver

  override def url = config.url
}

private[bigdataHbase] object MysqlConnection {
  def build(configName: String, rootNode: String): MysqlConnection = new MysqlConnection(configName, rootNode)
}


case class Db2Connection(configName: String, rootNode: String) extends JdbcConnection {

  def config = JDBCConfig.readConfig(configName, rootNode)

  require(config.driver.contains("db2"), "driver name must be db2")

  override def user = config.user

  override def password = config.password

  override def driver = config.driver

  override def url = config.url
}

private[bigdataHbase] object Db2Connection {
  def build(configName: String, rootNode: String): Db2Connection = new Db2Connection(configName, rootNode)

}


object JDBCConfig {

  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  def readConfig(configName: String, rootNode: String): JDBCConfig = {
    ConfigUtil.readClassPathConfig[JDBCConfig](configName, rootNode).build
  }
}


case class JDBCConfig(user: String,
                      password: String,
                      db: String,
                      schema: Option[String] = None,
                      driver: String,
                      host: String,
                      var url: String) {
  def build = appendArgs

  def appendArgs: JDBCConfig = {
    url = s"$url"
      .replace("$host", host)
      .replace("$db", db)
      .replace("$user", user)
      .replace("$password", password)
    schema match {
      case None => this
      case Some(sc) => url = url.replace("$schema", sc); this
    }
  }
}


object JdbcUtil {

  private lazy val numPartitions = 6

  def mysqlJdbcDF(table: String, rootNode: String = "source", numPartitions: Int = numPartitions, configName: String = "mysql2mysql")(implicit sqlContext: SQLContext): DataFrame = {
    val mysql = MysqlConnection.build(configName, rootNode)
    import mysql._
    sqlContext.jdbcDF(table, numPartitions)
  }

  def save2Mysql(table: String, numPartitions: Option[Int] = None, configName: String = "mysql2mysql", rootNode: String = "sink")(implicit dataFrame: DataFrame) = {
    val mysql = MysqlConnection.build(configName, rootNode)
    import mysql._
    dataFrame.save2DB(table, numPartitions /* orElse Option(1)*/)
  }

  def db2JdbcDF(table: String, numPartitions: Int = numPartitions, configName: String = "db2mysql", rootNode: String = "source")(implicit sqlContext: SQLContext): DataFrame = {
    val db2 = Db2Connection.build(configName, rootNode)
    import db2._
    sqlContext.jdbcDF(table, numPartitions)
  }

  def save2DB2(table: String, numPartitions: Option[Int] = None, configName: String = "db2mysql", rootNode: String = "source")(implicit dataFrame: DataFrame) = {
    val db2 = Db2Connection.build(configName, rootNode)
    import db2._
    dataFrame.save2DB(table, numPartitions /* orElse Option(1)*/)
  }
}