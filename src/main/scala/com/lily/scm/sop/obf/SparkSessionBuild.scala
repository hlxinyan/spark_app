package com.lily.scm.sop.obf

import com.lily.scm.sop.obf.config.AwsSparkConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionBuild {

  def build(appName: String): SparkSession = {

    val conf = new SparkConf()
      .setAppName(appName)
      .registerKryoClasses(Array(classOf[org.apache.spark.serializer.KryoSerializer]))
      .setAll(AwsSparkConfig.getConfigAsMap())

  val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

      spark

  }

}
