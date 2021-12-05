package cn.itcast.flink.start;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/**
 * 使用Flink DataStream实现词频统计WordCount，从Socket Source读取数据。
 */
public class _01StreamWordCount {
	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		// 2. 数据源-source
		DataStreamSource<String> lineDataStream = env.socketTextStream("node1", 9999);

		//env.fromElements("spark", "flink", "flink");

		// 3. 数据转换-transformation
		// 3.1 过滤数据，使用filter函数
		SingleOutputStreamOperator<String> filterOperator = lineDataStream.filter(
			new FilterFunction<String>() {
				@Override
				public boolean filter(String line) throws Exception {
					return null != line && line.trim().length() > 0;
				}
			}
		);
		// 3.2 转换数据，分割每行为单词
		SingleOutputStreamOperator<String> wordOperator = filterOperator.flatMap(
			new FlatMapFunction<String, String>() {
				@Override
				public void flatMap(String line, Collector<String> out) throws Exception {
					String[] words = line.trim().toLowerCase().split("\\W+");
					for (String word : words) {
						out.collect(word);
					}
				}
			}
		);
		// 3.3 转换数据，每个单词变为二元组
		SingleOutputStreamOperator<Tuple2<String, Integer>> tupleOperator = wordOperator.map(
			new MapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(String word) throws Exception {
					return Tuple2.of(word, 1);
				}
			}
		);
		// 3.4 分组
		SingleOutputStreamOperator<Tuple2<String, Integer>> countOperator = tupleOperator
			// 对DataStream数据流分组以后，进行聚合sum操作，此时状态就是KeyedState，针对每个单词都有1个State
			.keyBy(0)
			.sum(1);

		// 4. 数据终端-sink
		countOperator.printToErr();

		// 5. 触发执行-execute
		env.execute(_01StreamWordCount.class.getSimpleName());
	}
}