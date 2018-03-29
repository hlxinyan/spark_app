package com.lily.scm.sop.obf.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import com.lily.scm.sop.obf.config.CommonConfig.CustomizeConfig

protected[config] class ConfigLoader(val config: Config) {

  def this() = {
    this(ConfigFactory.load())

  }

  def this(resourceBasename: String) = {
    this(ConfigFactory.load(resourceBasename))
  }

  def this(resourceBasename: String, configName: String) = {
    this(ConfigFactory.load(resourceBasename).getConfig(configName))
  }

  override def toString = config.toString()
}


object  CommonConfig {

  val CONFIG_PATH = "config.conf"

  implicit class CustomizeConfig(conf: Config) {
    def getAsMap(): Map[String, String] = {
     (for {
        entry <-  conf.entrySet().asScala
        key = entry.getKey
        value = entry.getValue.unwrapped.toString()
      } yield (key, value)).toMap
    }
  }
}


abstract  class GenericConfig(configRootName:String,configName:String){
  //val CONFIG_ROOT_NAME = "generic-config"
  val config = loadConfig(configName)

  private def loadConfig(name: String): Config = {
    new ConfigLoader(CommonConfig.CONFIG_PATH, configRootName +"."+ configName).config
  }
  def getConfigAsMap(): Map[String, String] = {
    config.getAsMap()
  }

}


object AwsSparkConfig extends GenericConfig("aws-config","spark")

object GenericConfig extends GenericConfig("generic-config",""){
  lazy val version = config.getString("version")
}






