package com.itcast.spark.app.state

import com.itcast.spark.app.StreamingContextUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * 实时消费Kafka Topic数据，累加统计各个搜索词的搜索次数，实现百度搜索风云榜
 */
object _07StreamingUpdateState {

  def main(args: Array[String]): Unit = {

    // 1. 创建StreamingContext实例对象
    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 10)

    //TODO:状态计算,存储到CheckPoint中
    ssc.checkpoint("datas/streaming/ckpt-001")

    // 2. 从Kafka消费数据，采用New Consumer API
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    // 3. TODO： step1. 对当前批次数据进行聚合统计
    val reduceDStream: DStream[(String, Int)] = kafkaDStream.transform(rdd => {
      val reduceRDD: RDD[(String, Int)] = rdd
        //过滤
        .filter(record => null != record && null != record.value() && record.value().trim.split(",").length == 4)
        //提取关键词
        .map(record => (record
          .value().trim.split(",")(3), 1)
        )
        //聚合
        .reduceByKey(_ + _)
      //返回当前批次聚合RDD结果
      reduceRDD
    })

    // 3. TODO: step2. 将当前批次聚合结果与以前状态数据进行聚合操作（状态更新）
    /*
      def updateStateByKey[S: ClassTag](
          updateFunc: (Seq[V], Option[S]) => Option[S]
        ): DStream[(K, S)]
        - Seq[V]表示当前批次中Key对应的value值得集合
            如果对当前批次中数据按照Key进行聚合以后，此时，只有一个值
            V类型：Int
        - Option[S])：表示Key的以前状态，如果以前没有出现过该Key，状态就是None
            S类型：Int
     */
    val stateDStream: DStream[(String, Int)] = reduceDStream.updateStateByKey(
      (values: Seq[Int], state: Option[Int]) => {
        //获取当前批次中Key的状态
        val currentState: Int = values.sum
        //获取Key的以前状态
        val previousState: Int = state.getOrElse(0)
        //将当前状态与以前状态合并
        val lastestState: Int = currentState + previousState
        //返回最新状态
        Some(lastestState)
      }
    )
    // 4. 将每批次结果数据进行输出
    stateDStream.foreachRDD((rdd, time) => {

      val batchTime: String = FastDateFormat.getInstance("yyyyMMddHHmmssSSS").format(time.milliseconds)
      println("------------------------------")
      println(s"Batch Time: ${batchTime}")
      println("------------------------------")
      // 判断每批次结果RDD是否有数据，如果有数据，再进行输出
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).sortBy(_._2,false).foreachPartition(_.foreach(println))
      }
    })

    // 启动流式应用，等待终止结束
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
