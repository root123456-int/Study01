package cn.itcast.flink.sink.redis;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 案例演示：将数据保存至Redis中，直接使用官方提供Connector
 *      https://bahir.apache.org/docs/flink/current/flink-streaming-redis/
 */
public class _18StreamSinkRedisDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
		SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = inputDataStream
			// a. 过滤数据
			.filter(new FilterFunction<String>() {
				@Override
				public boolean filter(String line) throws Exception {
					return null != line && line.trim().length() > 0;
				}
			})
			// b. 分割单词
			.flatMap(new FlatMapFunction<String, String>() {
				@Override
				public void flatMap(String line, Collector<String> out) throws Exception {
					String[] words = line.trim().split("\\W+");
					for (String word : words) {
						out.collect(word);
					}
				}
			})
			// c. 转换二元组，表示每个单词出现一次
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(String word) throws Exception {
					return Tuple2.of(word, 1);
				}
			})
			// d. 按照单词分组及对组内聚合操作
			.keyBy(0).sum(1);
		resultDataStream.printToErr();

		// 4. 数据终端-sink
		/*
			spark -> 15
			flink -> 20
			hive -> 10
			--------------------------------------------
			Redis 数据结构：哈希Hash
				Key:
					flink:word:count
				Value: 哈希
					field  value
					spark  15
					flink  20
					hive   10
			命令：
				HSET flink:word:count spark 15

				SET name zhangsan
		 */


		// 5. 触发执行-execute
		env.execute(_18StreamSinkRedisDemo.class.getSimpleName()) ;
	}

}
