package com.itcast.spark.test

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * @author by FLX
 * @date 2021/11/7 0007 10:05.
 */
object SparkStreamingForeachRDDTest01 {

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

    resultDStream.foreachRDD((rdd, time) => // 此处RDD属于每批次分析结果RDD
      // 转换每批次时间
    {
      val batchTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        .format(time.milliseconds)
      println(s"----------------\nBatchTime:${batchTime}\n-------------------")

      //TODO: 判断结果RDD是否存在,存在再输出
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(iter =>
          iter.foreach(item => println(item))
        )
      }

    }
    )


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}
