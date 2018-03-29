package com.lily.scm.sop.obf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession


object SplitFileJob {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("testGetS3File")
      .registerKryoClasses(Array(classOf[org.apache.spark.serializer.KryoSerializer]))
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.endpoint", "s3-ap-northeast-2.amazonaws.com")
      .set("spark.hadoop.fs.s3a.connection.maximum", "1000")
      .set("spark.hadoop.fs.s3a.fast.upload", "true")
      .set("spark.hadoop.fs.s3a.connection.establish.timeout", "500")
      .set("spark.hadoop.fs.s3a.connection.timeout", "5000")
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.hadoop.com.amazonaws.services.s3.enableV4", "true")
      .set("spark.hadoop.com.amazonaws.services.s3.enforceV4", "true")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()




    val textFileRDD = spark.sparkContext.textFile(args(0));

    val count = textFileRDD.count()


    println (s"count==$count" )

  }

}
