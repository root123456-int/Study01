package com.itcast.spark.app.etl

import com.itcast.spark.app.StreamingContextUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

/**
 * 实时消费Kafka Topic数据，经过ETL（过滤、转换）后，保存至HDFS文件系统中，BatchInterval为：10s
 */
object _06StreamingETLHdfs {

  def main(args: Array[String]): Unit = {

    // 1. 创建StreamingContext实例对象
    val ssc: StreamingContext = StreamingContextUtils.getStreamingContext(this.getClass, 10)

    // 2. 从Kafka消费数据，采用New Consumer API
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")

    // TODO：3. 对获取数据，进行ETL转换，将IP地址转换为省份和城市
    /*
      1. 从message消息中获取ip地址
      2. 解析转换省份和城市
      3. 将省份和城市追加到原来数据
     */
    val etlDStream: DStream[String] = kafkaDStream.transform(rdd => { //TODO:此处RDD为每批次数据
      // 数据格式：a1e44ac71488fccd,121.76.107.140,20210621154830895,外籍女子拒戴口罩冲乘客竖中指
      val eltRDD: RDD[String] = rdd
        .filter(record => {
          null != record && null != record.value() && record.value().trim.split(",").length == 4
        })
        //转换ip地址
        .mapPartitions { iter =>
          //创建Dbsearcher对象
          val dbSearcher = new DbSearcher(new DbConfig(), "dataset/ip2region.db")
          iter.map { record =>
            //获取消息message数据
            val message: String = record.value().trim

            //TODO:解析IP地址
            //传递IP地址,解析获取数据
            val dataBlock: DataBlock = dbSearcher.btreeSearch(message.split(",")(1))

            //获取解析的省份和城市
            val region: String = dataBlock.getRegion
            val Array(_, _, province, city, _) = region.split("\\|")
            //将省份和城市追加到原原来数据上
            s"${message},${province},${city}"
          }
        }
      eltRDD
    })

    // 4. 保存数据至HDFS文件系统
    etlDStream.foreachRDD((rdd, time) => {
      val batchTime: String = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")
        .format(time.milliseconds)
      if (!rdd.isEmpty()) {
        //TODO:此处RDD就是每批次进行ETL转换后的结果,保存到HDFS文件系统时,降低分区数目
        rdd
          .coalesce(1)
          .saveAsTextFile(s"datas/etl/search-logs-${batchTime}")
      }
    })

    // 启动流式应用，等待终止结束
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
