package com.lily.scm.sop.obf

import breeze.numerics.log
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome, Suite}


trait SharedContext extends BeforeAndAfterAll { self:Suite=>

  def localConfig(): SparkConf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName(getClass().getName())
      .set("spark.akka.logLifecycleEvents", "true")
      .set("spark.sql.tungsten.enabled", "false")
      .set("spark.driver.memory", "512M")
      .set("spark.executor.memory", "512M")
      .set("spark.storage.memoryFraction", "0.3")
  }

  /**
    * To avoid Akka rebinding to the same port,
    * since it doesn't unbind immediately on shutdown
    */
  def clearSparkPortProperties(): Unit = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  /**
    * Log the suite name and the test name before and after each test.
    *
    * Subclasses should never override this method. If they wish to run
    * custom code before and after each test, they should mix in the
    * {{org.scalatest.BeforeAndAfter}} trait instead.
    * test: NoArgTest
    */
/*  final protected override def withFixture(): Outcome = {
    val testName ="test"
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
    try {
     // log.Info(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      //log.info(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }*/

}


trait SharedSparkContext extends SharedContext { self: Suite =>

  //
  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc


  override def beforeAll(): Unit = {
    clearSparkPortProperties()

    _sc = new SparkContext(localConfig())

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      if (_sc != null) {
        _sc.stop()
      }
      _sc = null

    } finally {
      clearSparkPortProperties()
    }
    super.afterAll()
  }
}


/**
  * Shares a local `SparkSession` between all tests in a suite
  */
trait SharedSparkSession extends SharedSparkContext { self: Suite =>
  @transient private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    super.beforeAll()

    _spark = SparkSession
      .builder()
      .config(localConfig())
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    _spark = null
  }

}


trait LocalSparkSession {

  def localConfig(): SparkConf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName(getClass().getName())
      .set("spark.akka.logLifecycleEvents", "true")
      .set("spark.sql.tungsten.enabled", "false")
      .set("spark.driver.memory", "512M")
      .set("spark.executor.memory", "512M")
      .set("spark.storage.memoryFraction", "0.3")
  }

  def spark: SparkSession = SparkSession
    .builder()
    .config(localConfig())
    .getOrCreate()
}