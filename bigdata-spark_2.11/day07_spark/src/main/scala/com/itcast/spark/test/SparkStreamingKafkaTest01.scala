package com.itcast.spark.test

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author by FLX
 * @date 2021/11/7 0007 14:41.
 */
object SparkStreamingKafkaTest01 {
  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = {
      val conf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")

      new StreamingContext(conf, Seconds(5))
    }

    //TODO: 2. 定义数据源,获取流式数据,封装DStream,
    // 从Kafka消费数据,采用New Consumer API方式

    /*
    def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): InputDStream[ConsumerRecord[K, V]]
     */

    //a. 设置消费kafka数据时,位置策略
    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
    /*
    def Subscribe[K, V](
          topics: Iterable[jl.String],
          kafkaParams: collection.Map[String, Object]): ConsumerStrategy[K, V]
     */
    /*
    val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "use_a_separate_group_id_for_each_stream",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)

val topics = Array("topicA", "topicB")
val stream = KafkaUtils.createDirectStream[String, String](
  streamingContext,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)

stream.map(record => (record.key, record.value))
     */
    //b. 设置消费kafka数据时,消费策略,传递基本参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1.itcast.cn:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_id_1001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(
      Array("wc-topic"),
      kafkaParams
    )

    //c. 采用New Consumer API消费Kafka Topic数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      locationStrategy,
      consumerStrategy
    )

    //仅仅获取Kafka Topic中的Value数据: Message消息
    val inputDStream: DStream[String] = kafkaDStream.map(recod => recod.value())

    val resultDStream: DStream[(String, Int)] = inputDStream.transform(rdd => {

      val resultRDD: RDD[(String, Int)] = rdd.filter(line => null != line && line.trim.length > 0)
        .flatMap(_.trim.split("\\s+"))
        .map((_, 1))
        .reduceByKey((_ + _))
      resultRDD
    }
    )

    resultDStream.foreachRDD((rdd, time) => {

      val batchTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        .format(time.milliseconds)
      println(s"-------------------\nBatchTime:${batchTime}\n-------------------")

      if (!rdd.isEmpty()) {
        rdd.foreach(println(_))
      }
    }
    )


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)

  }
}
