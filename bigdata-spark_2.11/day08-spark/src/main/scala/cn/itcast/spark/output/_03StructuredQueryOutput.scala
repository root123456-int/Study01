package cn.itcast.spark.output

import org.apache.spark.sql.functions.{explode, length, split, trim}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果打印到控制台。
 * 设置输出模式、查询名称、触发间隔及检查点位置
 */
object _03StructuredQueryOutput {

  def main(args: Array[String]): Unit = {

    // 构建SparkSession实例对象，相关配置进行设置
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._


    // 从TCP Socket加载数据，读取数据列名称为value，类型是String
    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1.itcast.cn")
      .option("port", "9999")
      .load()

    // 进行词频统计
    val resultDS: Dataset[Row] = inputStreamDF
      .filter($"value".isNotNull && length(trim($"value")) > 0)
      .select(explode(split(trim($"value"), "\\s+")).as("word"))
      .groupBy($"word").count()
      .orderBy($"count".desc)

    // 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    val query: StreamingQuery = resultDS.writeStream
      // TODO: a. 设置输出模式， 当数据更新时再进行输出
      .outputMode(OutputMode.Complete())
      // TODO: b. 设置查询名称
      .queryName("query-wordcount01")
      // TODO: c. 设置触发时间间隔
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      // TODO: d. 设置检查点目录
      .option("checkpointLocation", "datas/structed/ckpt-01")
      .start()


    // 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()
  }

}
