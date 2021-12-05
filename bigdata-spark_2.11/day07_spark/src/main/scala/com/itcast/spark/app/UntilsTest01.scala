package com.itcast.spark.app

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * @author by FLX
 * @date 2021/6/22 0022 18:15.
 */
object UntilsTest01 {
  def main(args: Array[String]): Unit = {
    //1.Steaming/context实例对象
    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 5)
    //2.消耗Kafka数据
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")
    //3.打印控制台
    kafkaDStream.print(50)

    //启动流式应用
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true,stopGracefully = true)

  }

}
