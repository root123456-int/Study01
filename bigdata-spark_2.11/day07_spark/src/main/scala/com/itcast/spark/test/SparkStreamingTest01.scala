package com.itcast.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author by FLX
 * @date 2021/11/6 0006 13:42.
 */
object SparkStreamingTest01 {

  def main(args: Array[String]): Unit = {

    // TODO:1. 创建StreamingContext对象
    val ssc: StreamingContext = {
      val conf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")

      new StreamingContext(conf, Seconds(5))
    }

    //TODO: 2. 定义数据源,获取流式数据,封装DStream

    /*
    def socketTextStream(
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String]
     */
    val inputDStream: DStream[String] = ssc.socketTextStream(
      "192.168.88.100",
      9999
    )

    //TODO: 3. 对此批次数据进行数据统计
    val resultDStream: DStream[(String, Int)] = inputDStream
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((tmp, item) => tmp + item)

    // TODO: 4. 将结果展示

//    println("---------------")
//    println(System.currentTimeMillis())
//    println("----------------")

    resultDStream.print()


    // TODO:5. 启动流式应用,等待终止
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}
