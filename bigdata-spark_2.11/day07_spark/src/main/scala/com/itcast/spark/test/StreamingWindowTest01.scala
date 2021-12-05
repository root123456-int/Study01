package com.itcast.spark.test

import com.itcast.spark.app.StreamingContextUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * 实时消费Kafka Topic数据,每隔一段时间统计最近搜索日志中搜索词次数
 * 批处理时间间隔: BatchInterval = 2s
 * 窗口大小间隔: WindowInterval = 4s
 * 滑动大小间隔: SliderInterval = 2s
 */
/**
 * @author by FLX
 * @date 2021/11/7 0007 19:25.
 */
object StreamingWindowTest01 {

  def main(args: Array[String]): Unit = {

    //1. 创建StreamingContext实例对象
    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 2)

    //TODO: 设置检查点目录
    ssc.checkpoint(s"datas/spark/ckpt-${System.nanoTime()}")

    //2. 从Kafka消费数据,采用New Consumer API
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    //TODO:设置窗口:大小为4s,滑动为2s
    /*
    def window(windowDuration: Duration, slideDuration: Duration): DStream[T]
     */
    val windowDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.window(Seconds(4), Seconds(2))

    //3. 对窗口中数据进行集合统计
    val resultDStream: DStream[(String, Int)] = windowDStream.transform { rdd =>
      val resultRDD: RDD[(String, Int)] = rdd
        .filter(record =>
          null != record && null != record.value() && record.value().trim.split(",").length == 4
        )
        .map(record =>
          record.value().trim.split(",")(3) -> 1
        )
        .reduceByKey(_ + _)
      resultRDD
    }

    resultDStream.foreachRDD((rdd, time) => {
      val formatTime: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:dd")

      println("-------------------------")
      println(s"BatchTime: ${formatTime.format(time.milliseconds)}")
      println("-------------------------")

      rdd.foreach(println(_))
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)

  }
}
