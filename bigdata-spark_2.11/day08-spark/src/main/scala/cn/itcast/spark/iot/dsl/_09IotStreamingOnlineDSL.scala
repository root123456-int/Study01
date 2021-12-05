package cn.itcast.spark.iot.dsl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 对物联网设备状态信号数据，实时统计分析:
 * 1）、信号强度大于30的设备
 * 2）、各种设备类型的数量
 * 3）、各种设备类型的平均信号强度
 */
object _09IotStreamingOnlineDSL {

  def main(args: Array[String]): Unit = {

    // 1. 构建SparkSession会话实例对象，设置属性信息
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    // 导入隐式转换和函数库
    import spark.implicits._
    // 2. 从Kafka读取数据，底层采用New Consumer API
    val iotStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1.itcast.cn:9092")
      .option("subscribe", "iotTopic")
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", "100000")
      .load()

    // 3. 对流式数据进行提取字段
    //{"device":"device_20","deviceType":"bigdata","signal":53.0,"time":1624347246593}
    //a. 获取message数据,转换为字符串
    //b.  提取每个字段的值
    val etlStreamDF: DataFrame = iotStreamDF
      //获取sessage数据,转换为字符串
      .selectExpr("CAST(value AS STRING)")
      //提取每个字段的值
      .select(
        get_json_object($"value", "$.device"),
        get_json_object($"value", "$.deviceType"),
        get_json_object($"value", "$.signal"),
        get_json_object($"value", "$.time")
      )

    // 4. 按照需求进行分析
    val resultStreamDF: DataFrame = etlStreamDF
      //1. 信号强度大于30的设备
      .filter($"signal" > 30)
      //2. 各种设备类型的数量和平均型号强度
      .groupBy($"deviceType")
      .agg(
        count($"deviceType").as("total"),
        round(avg($"signal"),2).as("avg_signal")
      )

    // 5. 启动流式应用，结果输出控制台
    val query: StreamingQuery = resultStreamDF
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .start()
    query.awaitTermination()
    query.stop()
  }

}
