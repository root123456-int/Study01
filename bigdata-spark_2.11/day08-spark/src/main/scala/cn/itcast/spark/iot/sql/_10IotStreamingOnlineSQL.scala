package cn.itcast.spark.iot.sql

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 对物联网设备状态信号数据，实时统计分析:
 * 1）、信号强度大于30的设备
 * 2）、各种设备类型的数量
 * 3）、各种设备类型的平均信号强度
 */
object _10IotStreamingOnlineSQL {

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

    val schema: StructType = new StructType()
      .add("device", StringType, true)
      .add("deviceType", StringType, true)
      .add("signal", DoubleType, true)
      .add("time", LongType, true)

    val etlStreamDF: DataFrame = iotStreamDF
      //a.提取value值,转换为String类型
      .select($"value".cast(StringType))
      //b.解析Json字符串
      .select(from_json($"value", schema).as("data"))
      //c.提取各个字段的值
      .select($"data.*")

    // 4. 按照需求进行分析

    //Step1.注册DataFrame为临时试图
    etlStreamDF.createOrReplaceTempView("view_temp_iot")

    //Step2.编写SQL并执行
    val resultStreamDF: DataFrame = spark.sql(
      """
        |SELECT
        |  deviceType, COUNT(1) AS total, ROUND(AVG(signal), 2) AS avg_signal
        |FROM
        |  view_temp_iot
        |WHERE
        |  signal > 30
        |GROUP BY
        |  deviceType
        |""".stripMargin
    )

    // 5. 启动流式应用，结果输出控制台
    val query: StreamingQuery = resultStreamDF.writeStream
      // 输出模式
      .outputMode(OutputMode.Complete())
      // 每个微批次输出
      .foreachBatch((batchDF: DataFrame, batchID: Long) =>
        batchDF.coalesce(1).show(10, false)
      )
      .start()

    query.awaitTermination()
    query.stop()
  }

}
