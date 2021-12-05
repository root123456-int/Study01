package cn.itcast.spark.test01

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
 * @author by FLX
 * @date 2021/11/8 0008 20:27.
 */
object StructuredStreamingWindowTest01 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1.itcast.cn")
      .option("port", 9999)
      .load()

    val etlStreamDF: DataFrame = inputStreamDF
      .as[String]
      .filter(line => null != line && line.trim.split(",").length == 2)
      .flatMap { line =>
        val arr: Array[String] = line.trim.split(",")
        arr(1)
          .split("\\s+")
          .map(word => (Timestamp.valueOf(arr(0)), word))
      }
      .toDF("insert_timestamp", "word")

    val resultStreamDF: DataFrame = etlStreamDF
      .groupBy(
        window($"insert_timestamp", "10 seconds", "5 seconds")
        , $"word"
      ).count()
      .orderBy($"window")

    resultStreamDF.printSchema()

    val query: StreamingQuery = resultStreamDF.writeStream
      .outputMode(OutputMode.Complete())
      .queryName("query-window")
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
    query.stop()

  }

}
