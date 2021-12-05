package cn.itcast.spark.start

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果打印到控制台。
 * 第一点、程序入口SparkSession，加载流式数据：spark.readStream
 * 第二点、数据封装Dataset/DataFrame中，分析数据时，建议使用DSL编程，调用API，很少使用SQL方式
 * 第三点、启动流式应用，设置Output结果相关信息、start方法启动应用
 */
object _01StructuredWordCount {

  def main(args: Array[String]): Unit = {

    // TODO: step1. 构建SparkSession实例对象，相关配置进行设置
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._

    // TODO: step2. 从TCP Socket加载数据，读取数据列名称为value，类型是String
    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1.itcast.cn")
      .option("port", "9999")
      .load()


    // TODO: step3. 进行词频统计
    val resultStreamDF: DataFrame = inputStreamDF

      //过滤数据
      .filter(
        $"value".isNotNull && length(trim($"value")).gt(0)
      )
      //分隔每行数据为单词
      .select(
        explode(
          split(trim($"value"), "\\s+")
        ).as("word")
      )
      .groupBy($"word").count()


    // TODO: step4. 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    val query: StreamingQuery = resultStreamDF.writeStream
      //设置输出模式,表示将RedultTable中数据如何输出
      .outputMode(OutputMode.Update()) //当数据更新时,进行输出
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .start()

    // TODO: step5. 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()
  }
}
