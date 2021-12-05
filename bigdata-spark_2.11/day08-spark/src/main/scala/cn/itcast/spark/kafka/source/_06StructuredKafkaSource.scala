package cn.itcast.spark.kafka.source

import org.apache.spark.sql.functions.{explode, length, split, trim}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用Structured Streaming从Kafka实时读取数据，进行词频统计，将结果打印到控制台。
 */
object _06StructuredKafkaSource {

  def main(args: Array[String]): Unit = {

    // 构建SparkSession实例对象，相关配置进行设置
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      // 设置Shuffle时分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._

    // TODO: 从Kafka 加载数据
    val kafkaStreamDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","node1.itcast.cn:9092")
      .option("subscribe","wordsTest")
      .load()

    // 进行词频统计
    val resultStreamDF: DataFrame = kafkaStreamDF
      //获取value的值,转换为String类型
      .selectExpr("CAST(value AS STRING)")
      .filter($"value".isNotNull && length(trim($"value")) > 0)
      // hadoop spark hadoop spark spark  -> 分割单词，并且扁平化
      .select(
        explode(split(trim($"value"), "\\s+")).as("word")
      )
      .groupBy("word")
      .count()

    // 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    val query: StreamingQuery = resultStreamDF.writeStream
      // 设置输出模式， 当数据更新时再进行输出： mapWithState
      .outputMode(OutputMode.Update())
      // 设置查询名称
      .queryName("query-wordcount-kafka")
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      // 设置检查点目录
      .option("checkpointLocation", s"datas/spark/structured-ckpt-${System.nanoTime()}")
      .start()
    // 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()
  }

}
