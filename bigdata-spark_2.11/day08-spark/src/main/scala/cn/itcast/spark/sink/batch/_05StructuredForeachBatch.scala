package cn.itcast.spark.sink.batch

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 使用Structured Streaming从TCP Socket实时读取数据，进行词频统计，将结果存储到MySQL数据库表中
 */
object _05StructuredForeachBatch {

  def main(args: Array[String]): Unit = {

    // 构建SparkSession实例对象，相关配置进行设置
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      // 设置Shuffle时分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._

    // 从TCP Socket加载数据，读取数据列名称为value，类型是String
    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1.itcast.cn")
      .option("port", 9999)
      .load()

    // 进行词频统计
    val resultStreamDF: DataFrame = inputStreamDF
      .filter($"value".isNotNull && length(trim($"value")) > 0)
      // hadoop spark hadoop spark spark  -> 分割单词，并且扁平化
      .select(
        explode(split(trim($"value"), "\\s+")).as("word")
      )
      .groupBy("word")
      .count()

    // 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    val query: StreamingQuery = resultStreamDF.writeStream
      // a. 设置输出模式， 当数据更新时再进行输出： mapWithState
      .outputMode(OutputMode.Complete())
      // b. 设置查询名称
      .queryName("query-wordcount")
      // c. 设置触发时间间隔
      .trigger(Trigger.ProcessingTime(0, TimeUnit.SECONDS))
      // TODO: 使用foreachBatch方法，将结果数据，写入MySQL表中
      /*
     def foreachBatch(function: VoidFunction2[Dataset[T], java.lang.Long]): DataStreamWriter[T] = {
    foreachBatch((batchDs: Dataset[T], batchId: Long) => function.call(batchDs, batchId))
  }
       */
      .foreachBatch((batchDF:DataFrame,batchID:Long) => {
        batchDF
          .coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .option("driver","com.mysql.cj.jdbc.Driver")
          .option("user","root")
          .option("password","123456")
          .jdbc(
            "jdbc:mysql://192.168.88.100:3306/db_spark?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
            "db_spark.tb_word_count_v2",
            new Properties()
          )
      })
      // d. 设置检查点目录
      .option("checkpointLocation", s"datas/spark/structured-ckpt-${System.currentTimeMillis()}")
      .start()
    // 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()
  }

}
