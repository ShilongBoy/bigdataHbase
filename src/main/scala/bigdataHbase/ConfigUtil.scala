package bigdataHbase

import java.io.File
import java.net.URLDecoder
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

/**
  * Created by Sean on 2016/7/1.
  */
object ConfigUtil {

  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def readFileConfig[T: ValueReader](path: String, root: String = "task") = {
    val myConfigFile = new File(path)
    val fileConfig = ConfigFactory.parseFile(myConfigFile)
    val config: Config = ConfigFactory.load(fileConfig)
    val tasks = config.as[T](root)
    tasks
  }

  def readClassPathConfig[T: ValueReader](configFileName: String, root: String = "task") = {
    val config: Config = ConfigFactory.load(configFileName)
    val tasks = config.as[T](root)
    tasks
  }


  def readEncodeStringConfig[T: ValueReader](encodeString: String) = {
    val configJson = URLDecoder.decode(encodeString, "UTF-8")
    readStringConfig[T](configJson)
  }

  def readStringConfig[T: ValueReader](string: String) = {
    println(s"parameters:\n\r${string}")
    val root = string.split(" ")(0).replaceAll("\n", "")
    val config: Config = ConfigFactory.parseString(string)
    val task = config.as[T](root)
    println(s"parse result:\r\n${(root, task)}")
    (root, task)
  }

  def readEncodingJson[T](string: String)(implicit clazz: Class[T]) = {
    val configJson = URLDecoder.decode(string, "UTF-8")
    readJson(configJson)
  }

  def readJson[T](configJson: String)(implicit clazz: Class[T]) = {
    objectMapper.readValue(configJson, clazz)
  }

  def map2Class[T](m: AnyRef)(implicit clazz: Class[T]) = {
    val str = objectMapper.writeValueAsString(m)
    objectMapper.readValue(str, clazz)
  }



}



