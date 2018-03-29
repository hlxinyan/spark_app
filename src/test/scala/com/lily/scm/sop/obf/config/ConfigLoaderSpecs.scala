package com.lily.scm.sop.obf.config

import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class ConfigLoaderSpecs extends FunSpec
  with GeneratorDrivenPropertyChecks {

  describe("Logger.getLogger(getClass().getName())") {

    describe("getAsMap spark.hadoop.fs.s3a.implp") {
      it("should return config as Map of Strings") {
        val config = new ConfigLoader("config.conf", "aws-config.spark").config

        assertResult("org.apache.hadoop.fs.s3a.S3AFileSystem") {
          config.getString("spark.hadoop.fs.s3a.impl")
         }
      }
    }

  }
}
