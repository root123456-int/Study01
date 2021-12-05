package com.itcast.spark.test

import com.itcast.spark.app.StreamingContextUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * @author by FLX
 * @date 2021/11/7 0007 16:21.
 */
object StreamingUpadateStateByKeyTest01 {

  def main(args: Array[String]): Unit = {

    //1. 创建StreamingContext实例对象
    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 10)

    //TODO: 状态计算,上一个状态存储到CheckPoint
    ssc.checkpoint("datas/streaming/ckpt-10001")
    //2. 从Kafka消费数据,采用New Consumer API
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    //3. TODO:a. 对当前批次数据进行聚合
    val reduceDStream: DStream[(String, Int)] = kafkaDStream.transform(rdd => {
      //数据格式：a1e44ac71488fccd,121.76.107.140,20210621154830895,外籍女子拒戴口罩冲乘客竖中指

      val reduceRDD: RDD[(String, Int)] = rdd.filter(record => {
        null != record && null != record.value() && record.value().trim.split(",").length == 4
      })
        //提取搜索关键词
        .map(record => {
          record.value().trim.split(",")(3) -> 1
        })

        //按照搜索词分组聚合统计
        .reduceByKey(_ + _)


      reduceRDD
    })

    /*
    def updateStateByKey[S: ClassTag](
    updateFunc: (Seq[V], Option[S]) => Option[S]
  ): DStream[(K, S)]
     */
    val stateDStream: DStream[(String, Int)] = reduceDStream.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      //i: 获取当前批次中key的状态
      val currentState: Int = values.sum

      //ii: 获取Key的以前状态值
      val previousState: Int = state.getOrElse(0)

      //iii: 将当前状态和以前状态合并
      val lastestState: Int = currentState + previousState

      //iv. 返回最新状态
      Some(lastestState)
    })

    stateDStream.foreachRDD((rdd, time) => {

      val batchTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        .format(time.milliseconds)

      println(s"--------------------\nBatchTime: ${batchTime}\n---------------------")

      if (!rdd.isEmpty()) {
        rdd.coalesce(1).foreach(println(_))
      }

    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}
