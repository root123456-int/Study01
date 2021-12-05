package cn.itcast.spark.kafka.sink

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 实时从Kafka Topic消费基站日志数据，过滤获取通话转态为success数据，再存储至Kafka Topic中
	 * 1、从KafkaTopic中获取基站日志数据（模拟数据，JSON格式数据）
	 * 2、ETL：只获取通话状态为success日志数据
	 * 3、最终将ETL的数据存储到Kafka Topic中
 */
object _07StructuredEtlSink {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			// 设置Shuffle分区数目
			.config("spark.sql.shuffle.partitions", "3")
			.getOrCreate()
		// 导入隐式转换和函数库
		import spark.implicits._
	
		// TODO: 1. 从KafkaTopic中获取基站日志数据（模拟数据，文本数据）
		val kafkaStreamDF: DataFrame = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers","node1.itcast.cn:9092")
			.option("subscribe","stationTopic")
			//设置每批次处理的最大数据量
			.option("maxOffsetsPerTrigger","20000")
			.load()
		
		// TODO: 2. ETL：只获取通话状态为success日志数据
		val eltStreamDS: Dataset[String] = kafkaStreamDF
			//先过滤非空数据
			.filter($"value".isNotNull)
			//转换value值为string类型
			.select($"value".cast(StringType))
			.as[String]
			.filter { message =>
				val arr: Array[String] = message.trim.split(",")
				arr.length == 6 && "success".equals(arr(3))
			}
		
		// TODO: 3. 最终将ETL的数据存储到Kafka Topic中
		val query: StreamingQuery = eltStreamDS
			.writeStream
			.queryName("query-state-etl")
			.outputMode(OutputMode.Append())
			// TODO：将数据保存至Kafka Topic中
			.format("kafka")
			.option("kafka.bootstrap.servers","node1.itcast.cn:9092")
			.option("topic","etlTopic")
			//设置检查点目录
			.option("checkpointLocation","datas/structed/ckpt-kafka01")
			.start()
		query.awaitTermination()
		query.stop()
	}
	
}
