package cn.itcast.flink.start

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * 使用Scala语言编写从TCP Socket读取数据，进行词频统计WordCount，结果打印至控制台。
 */
object _02FlinkWordCount {
	
	def main(args: Array[String]): Unit = {
		// 1. 执行环境-env
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(2)

		// 2. 数据源-source
		val inputDStream: DataStream[String] = env.socketTextStream("192.168.88.101", 9999)

		// 3. 数据转换-transformation
		val resultDStream: DataStream[(String, Int)] = inputDStream
			.filter(line => null != line && line.trim.length > 0)
			.flatMap(line => line.trim.split("\\W+"))
			.map((_, 1))
			.keyBy(0)
			.sum(1)
		
		// 4. 数据终端-sink
		resultDStream.printToErr()

		// 5. 触发执行-execute
		env.execute(this.getClass.getSimpleName)
	}
	
}
