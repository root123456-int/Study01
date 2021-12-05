package cn.itcast.flink.eventtime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口统计案例演示：滚动事件时间窗口（Tumbling EventTime Window），窗口内数据进行词频统计
 */
public class _08StreamTumblingEventTimeWindow {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;
		// TODO: step1. 设置时间语义为事件时间EventTime
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// 2. 数据源-source
		SingleOutputStreamOperator<String> inputDStream = env.socketTextStream("node1", 9999)
				.filter(line -> null != line && line.trim().split(",").length == 3);

		// TODO: step2. 设置事件时间字段，数据类型必须为Long类型
		SingleOutputStreamOperator<String> longTypeDStream = inputDStream
				.assignTimestampsAndWatermarks(
						// 此时，不允许数据延迟，如果延迟，就不处理数据
						new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
							@Override
							public long extractTimestamp(String line) {
								return Long.parseLong(line.trim().split(",")[0]);
							}
						}
				);

		// 3. 数据转换-transformation
/*
1000,a,1
2000,a,1
5000,a,1
9999,a,1
11000,a,2
14000,b,1
14999,b,1
 */
		SingleOutputStreamOperator<Tuple2<String, Integer>> resultDStream = longTypeDStream
				.map(new MapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> map(String line) throws Exception {
						String[] array = line.trim().split(",");

						return Tuple2.of(array[1], Integer.valueOf(array[2]));
					}
				})
				// TODO: step3. 设置窗口: 5秒
				.keyBy(0)
				.timeWindow(Time.seconds(5))
				.sum(1);


		// 4. 数据终端-sink
		resultDStream.printToErr();

		// 5. 触发执行-execute
		env.execute(_08StreamTumblingEventTimeWindow.class.getSimpleName()) ;
	}

}
