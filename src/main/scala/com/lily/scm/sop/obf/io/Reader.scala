package com.lily.scm.sop.obf.io

import org.apache.spark.sql.SparkSession

case class Reader(implicit  sparkSession:SparkSession) {

  def getSpark():SparkSession={
    sparkSession
  }
  def loadTestData():List[Int]={
    val list=(0 until 10).toList
    list
  }

}
