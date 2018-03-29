package com.lily.scm.sop.obf

import com.lily.scm.sop.obf.io.{Reader, Writer}
import org.apache.log4j.Logger



class AccumulatorConfigurableSparkJob extends BaseConfigurableSparkJob {

  override val log =Logger.getLogger(getClass().getName()) // LoggerFactory.getLogger(getClass().getName())

  override val jobName = "CustomizeAccumulatorJob"
  override val jobDescription =
    """|This job test accumulator function
       |to get global result.\n"""

  def compute(reader: Reader, writer: Writer, config: ConfigT): Unit={


    val testData=reader.loadTestData()

    val sparkSession=reader.getSpark()
    val sc=sparkSession.sparkContext;
    val accu=sc.doubleAccumulator("result")

      val rdd= sc.parallelize(testData)

      rdd.foreach(x=>{
          accu.add(x)

      });

    println(accu.value)

  }


  def defaultConfig = BaseConfig("20180101", "20180101000000", 2);

}
