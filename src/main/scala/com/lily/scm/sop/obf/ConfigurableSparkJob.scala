package com.lily.scm.sop.obf

import com.lily.scm.sop.obf.config.GenericConfig
import com.lily.scm.sop.obf.io.{Reader, Writer}
import org.apache.log4j.Logger

import scala.util.Try


trait SparkJob {
  val jobName: String
  val jobDescription: String
  val log:Logger
}


trait ConfigurableSparkJob extends SparkJob {

  protected type ConfigT <: Config

  def parser: scopt.OptionParser[ConfigT]
  def defaultConfig: ConfigT

  def main(args: Array[String]): Unit = {

    log.info(s"Running spark version ${GenericConfig.version}")

    parser.parse(args, defaultConfig) match {
      case Some(config) => run(config)
      case None         => log.error(s"Invalid input parameters, cannot start $jobName")
    }
  }

  def run(config: ConfigT): Unit
}


protected[obf] abstract class SimpleConfigurableSparkJob extends ConfigurableSparkJob {

  def compute(reader: Reader, writer: Writer, config: ConfigT): Unit

  override def run(config: ConfigT): Unit = {
    val spark = SparkSessionBuild.build(s"$jobName for $config")

    try {
      compute(Reader()(spark), Writer()(spark), config)

    } finally {
      Try(spark.stop) //ignore if any error is thrown here
    }
  }
}


/**
  * Spark job with 3 configurable input parameters:
  * <li> dt: date of the data to process
   * <li> nb of partitions: optional performance tuning parameter
  */
abstract class BaseConfigurableSparkJob extends SimpleConfigurableSparkJob {

  protected type ConfigT = BaseConfig

  override def parser = new scopt.OptionParser[BaseConfig](jobName) {
    head(jobName)
    opt[String]("dt") required () valueName ("<date in format yyyyMMdd>") action {
      (x, c) => c.copy(dt = x)
    } text ("Date of data to process")


    opt[String]("report_id") required () valueName ("<identify of Report>") action {
      (x, c) => c.copy(reportId = x)
    } text ("Identify of report")

    opt[Int]("nbPartitions") required () valueName ("<number of Partitions>") action {
      (x, c) => c.copy(nbPartitions = x)
    } text ("number of Partitions")


    opt[Map[String, String]]('x', "extra-conf") optional () valueName ("k1=v1,k2=v2...") action {
      (x, c) => c.copy(extras = x)
    } text ("Extra configuration parameters")

    note(jobDescription.stripMargin)
    help("help") text ("prints this usage text")
  }

}


trait Config {
  val extras: Map[String, String] = Map()

  protected[this] def extrasToString(): String = {
    if (extras.isEmpty) ""
    else s", Extra params: ${extras.toString()}"
  }

}

case class BaseConfig(dt: String, reportId: String, nbPartitions: Int,
                      override val extras: Map[String, String] = Map()) extends Config {

  override def toString = {
    val builder = new StringBuilder(s"Date: $dt")
    builder.append(s", reportId: $reportId")
    builder.append(extrasToString)
    builder.toString
  }
}












