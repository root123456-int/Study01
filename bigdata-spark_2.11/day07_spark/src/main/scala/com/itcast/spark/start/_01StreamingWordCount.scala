package com.itcast.spark.start

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 基于IDEA集成开发环境，编程实现从TCP Socket实时读取流式数据，对每批次中数据进行词频统计。
 */
object _01StreamingWordCount {

  def main(args: Array[String]): Unit = {

    // TODO: 1. 构建StreamingContext实例对象，传递时间间隔BatchInterval
    val ssc: StreamingContext = {
      //1.创建SparkConf对象,设置时间间隔BatchInterval
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("&"))
        .setMaster("local[3]")
      //传递SaprkConf对象,和时间间隔batchInterval:5秒
      new StreamingContext(sparkConf, Seconds(5))
    }

    // TODO: 2. 定义数据源，获取流式数据，封装到DStream中
    /*
    def socketTextStream(
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String]
     */
    //一行一行读取Socket中数据
    val inputDStream: DStream[String] = ssc.socketTextStream("192.168.88.100", 9999)


    // TODO: 3. 依据业务需求，调用DStream中转换函数（类似RDD中转换函数）
    /*
        spark hive hive spark spark hadoop
     */
    val resultDStream: DStream[(String, Int)] = inputDStream
      .filter(line => null != line && line.trim.split("\\s+").length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)

    // TODO: 4. 定义数据终端，将每批次结果数据进行输出

    resultDStream.print()

    // TODO: 5. 启动流式应用，等待终止

    ssc.start()
    //当流式应用启动以后,一直运行,除非人为中止或程序异常,否则一直等待关闭
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
