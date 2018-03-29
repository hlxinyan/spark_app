package com.lily.scm.sop.obf

import com.lily.scm.sop.obf.io.{Reader, Writer}
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.{FunSpec, Outcome}
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks


@RunWith(classOf[JUnitRunner])
class AccumulatorConfigurableSparkJobTest extends FunSpec
  with LocalSparkSession
  with GeneratorDrivenPropertyChecks {


  describe("Logger.getLogger(getClass().getName())") {

    describe("accumulate ") {
      it("should return global accumulate value") {
        val sparkSession = super.spark

        val accu = new AccumulatorConfigurableSparkJob();

        val reader = Reader()(sparkSession);

        val writer = Writer()(sparkSession)
        accu.compute(reader, writer, accu.defaultConfig)


      }
    }
  }


}
