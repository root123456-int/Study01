package com.itcast.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author by FLX
 * @date 2021/11/7 0007 9:45.
 */
object SparkStreamingTransformRDDTest01 {
  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = {
      val conf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

      new StreamingContext(conf, Seconds(5))
    }

    val inputDStream: DStream[String] = ssc.socketTextStream(
      "192.168.88.100",
      9999,
      StorageLevel.MEMORY_AND_DISK
    )

    val resultDStream: DStream[(String, Int)] = inputDStream.transform { rdd =>
      val resultRDD: RDD[(String, Int)] = rdd.filter(line => null != line && line.trim.length > 0)
        .flatMap(line => line.trim.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
      resultRDD
    }

    resultDStream.print()



    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)
  }
}
