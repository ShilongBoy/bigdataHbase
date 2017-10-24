package bigdataHbase

import scala.collection.mutable.ArrayBuffer

object Test {

  def main(args: Array[String]): Unit = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    val rs=ConfigUtil.readClassPathConfig[Mysql]("mysql2mysql","source")
    print(rs.url)



  }

  case class Mysql(user:String,password:String,db:String,driver:String,host:String,url:String)

}
