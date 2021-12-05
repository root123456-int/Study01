package cn.itcast.flink.window.count;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

/**
 * 窗口统计案例演示：滚动计数窗口（Tumbling Count Window)，数字累加求和统计
 */
public class _04StreamTumblingCountWindow {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.socketTextStream("node1", 9999);

		// 3. 数据转换-transformation
/*
数据格式：
1
1
1
2
3
4
5
5
 */
		SingleOutputStreamOperator<Tuple1<Integer>> resultDStream = inputDataStream
				.filter(line -> null != line && line.trim().length() > 0)
				.map(new MapFunction<String, Tuple1<Integer>>() {
					@Override
					public Tuple1<Integer> map(String value) throws Exception {

						return Tuple1.of(Integer.parseInt(value));
					}
				})

				// TODO: 滚动计数窗口设置，不进行key分组，使用windowAll
				//TODO:此语句意思是:3条一计算,计算最近5条的,开始的时候满3条一计算,到了下一个满三的时候,就计算最近5条的
				.countWindowAll(5)
				.sum(0);


		// 4. 数据终端-sink
		resultDStream.printToErr();

		// 5. 触发执行-execute
		env.execute(_04StreamTumblingCountWindow.class.getSimpleName()) ;
	}

}
