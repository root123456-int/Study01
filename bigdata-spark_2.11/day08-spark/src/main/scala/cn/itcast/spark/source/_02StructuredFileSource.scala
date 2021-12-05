package cn.itcast.spark.source

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用Structured Streaming从目录中读取文件数据：统计年龄小于25岁的人群的爱好排行榜
 */
object _02StructuredFileSource {

  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象，相关配置进行设置
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      // 设置Shuffle时分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._

    // TODO: 从文件数据源加载数据，本质就是监控目录
    //定义schema信息,映射到csv文件中
    val schema: StructType = StructType(
      Array(
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("hobby", StringType, nullable = true)
      )
    )
		val inputStreamDF: DataFrame = spark.readStream
			.format("csv")
			.schema(schema)
			.option("sep", ";")
			.load("file:///E:/datas/")

    // TODO: 监听某一个目录，读取csv格式数据，统计年龄小于25岁的人群的爱好排行榜。
    val resultStreamDF: DataFrame = inputStreamDF
			.filter($"age" < 25 )
			.groupBy($"hobby").count()
			.orderBy($"count".desc)

    // TODO: 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    val query: StreamingQuery = resultStreamDF.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .start()
    // 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()
  }

}
