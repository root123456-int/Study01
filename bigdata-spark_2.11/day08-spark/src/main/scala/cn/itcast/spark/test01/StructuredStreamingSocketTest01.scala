package cn.itcast.spark.test01


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
 * @author by FLX
 * @date 2021/11/8 0008 9:39.
 */
object StructuredStreamingSocketTest01 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[3]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1.itcast.cn")
      .option("port", "9999")
      .load()

    val resultStreamDF: DataFrame = inputStreamDF
      .filter(
        $"value".isNotNull && length(trim($"value")).gt(0)
      )
      .select(
        explode(
          split(trim($"value"), "\\s+")
        )
      ).as("word")
      .groupBy($"word")
      .count()

    val query: StreamingQuery = resultStreamDF.writeStream
      //TODO:1. 设置输出模式
      .outputMode(OutputMode.Update())
      //TODO:2. 设置查询名称
      .queryName("query_wordcount")
      //TODO:3. 设置出发时间间隔
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      //TODO:4. 设置检查点目录
      .option("checkpointLocation","datas/structured/ckpt-out-1001")
      .start()

    query.awaitTermination()
    query.stop()


  }
}
