package com.itcast.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 基于IDEA集成开发环境，编程实现从TCP Socket实时读取流式数据，对每批次中数据进行词频统计。
 */
object _03StreamingTransformRDD {

  def main(args: Array[String]): Unit = {

    // TODO: 1. 构建StreamingContext实例对象，传递时间间隔BatchInterval
    val ssc: StreamingContext = {
      // a. 创建SparkConf对象，设置应用基本信息
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("&"))
        .setMaster("local[3]")
      // b. 创建实例对象，设置BatchInterval
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
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream(
      "192.168.88.100",
      9999,
      storageLevel = StorageLevel.MEMORY_AND_DISK
    )

    // TODO: 3. 依据业务需求，调用DStream中转换函数（类似RDD中转换函数）
    /*
      TODO: 能对RDD操作的就不要对DStream操作，当调用DStream中某个函数在RDD中也存在，使用针对RDD操作
      def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
     */
    // 此处rdd就是DStream中每批次RDD数据
    val resultDStream: DStream[(String, Int)] = inputDStream.transform(rdd => {
      rdd
        .filter(line => null != line && line.trim.split("\\s+").length > 0)
        .flatMap(_.trim.split("\\s+"))
        .map((_, 1))
        .reduceByKey(_ + _)
    }
    )


    // TODO: 4. 定义数据终端，将每批次结果数据进行输出
    resultDStream.print()

    // TODO: 5. 启动流式应用，等待终止
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
