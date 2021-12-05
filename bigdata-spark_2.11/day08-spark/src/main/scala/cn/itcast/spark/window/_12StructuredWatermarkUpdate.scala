package cn.itcast.spark.window

import java.sql.Timestamp

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 基于Structured Streaming 读取TCP Socket读取数据，事件时间窗口统计词频，将结果打印到控制台
 * 	    TODO：每5秒钟统计最近10秒内的数据（词频：WordCount)，设置水位Watermark时间为10秒
			dog,2019-10-10 12:00:07
			owl,2019-10-10 12:00:08
			
			dog,2019-10-10 12:00:14
			cat,2019-10-10 12:00:09
			
			cat,2019-10-10 12:00:15
			dog,2019-10-10 12:00:08
			owl,2019-10-10 12:00:13
			owl,2019-10-10 12:00:21
			
			owl,2019-10-10 12:00:17
 */
object _12StructuredWatermarkUpdate {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 构建SparkSession实例对象，传递sparkConf参数
		val spark: SparkSession =  SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[2]")
			.config("spark.sql.shuffle.partitions", "2")
			.getOrCreate()
		// b. 导入隐式转换及函数库
		import org.apache.spark.sql.functions._
		import spark.implicits._
		
		// 2. 使用SparkSession从TCP Socket读取流式数据
		val inputStreamDF: DataFrame = spark.readStream
			.format("socket")
			.option("host", "node1.itcast.cn")
			.option("port", 9999)
			.load()
		
		// 3. 针对获取流式DataFrame设置EventTime窗口及Watermark水位限制
		val etlStreamDF: DataFrame = inputStreamDF
			// 将DataFrame转换为Dataset操作，Dataset是类型安全，强类型
			.as[String]
			// 过滤无效数据
			.filter(line => null != line && line.trim.length > 0)
			// 将每行数据进行分割单词: 2019-10-12 09:00:02,cat dog
			.map{line =>
				val arr = line.trim.split(",")
				(arr(0), Timestamp.valueOf(arr(1)))
			}
			// 设置列的名称
			.toDF("word", "time")
			
		val resultStreamDF = etlStreamDF
			// TODO：设置水位Watermark
			.withWatermark("time", "10 seconds")
			// TODO：设置基于事件时间（event time）窗口 -> time, 每5秒统计最近10秒内数据
			.groupBy(
				window($"time", "10 seconds", "5 seconds"),
				$"word"
			).count()
		
		// 4. 将计算的结果输出，打印到控制台
		val query: StreamingQuery = resultStreamDF.writeStream
			.outputMode(OutputMode.Update())
			.format("console")
			.option("numRows", "100")
			.option("truncate", "false")
			.trigger(Trigger.ProcessingTime("5 seconds"))
			.start()  // 流式DataFrame，需要启动
		// 查询器一直等待流式应用结束
		query.awaitTermination()
		query.stop()
	}
	
}
