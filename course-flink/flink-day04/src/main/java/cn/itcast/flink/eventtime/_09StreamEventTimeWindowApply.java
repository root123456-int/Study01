package cn.itcast.flink.eventtime;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口统计案例演示：滚动事件时间窗口（Tumbling EventTime Window），窗口内数据进行词频统计
 */
public class _09StreamEventTimeWindowApply {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;
		// TODO: step1. 设置时间语义为事件时间EventTime
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// 2. 数据源-source
		SingleOutputStreamOperator<String> filterDataStream = env
			.socketTextStream("node1.itcast.cn", 9999)
			.filter(line -> null != line && line.trim().split(",").length == 3);

		// TODO: step2. 设置事件时间字段，数据类型必须为Long类型
		SingleOutputStreamOperator<String> timeDataStream = filterDataStream.assignTimestampsAndWatermarks(
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
		SingleOutputStreamOperator<Tuple2<String, Integer>> countDataStream = timeDataStream
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss") ;
				@Override
				public Tuple2<String, Integer> map(String line) throws Exception {
					String[] split = line.trim().split(",");
					// 打印每条数据
					System.out.println(format.format(Long.parseLong(split[0])) + "," + split[1] + "," + split[2]);
					return Tuple2.of(split[1], Integer.parseInt(split[2]));
				}
			})
			// 先分组
			.keyBy(0)  // 元组数据类型是，使用下标索引
			// TODO: step3. 设置窗口: 5秒
			.timeWindow(Time.seconds(5))
			// 窗口内数据聚合
			.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
				private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss") ;

				@Override
				public void apply(Tuple tuple,
				                  TimeWindow window,
				                  Iterable<Tuple2<String, Integer>> input,
				                  Collector<Tuple2<String, Integer>> out) throws Exception {
					// 获取分组Key
					String  key = (String)((Tuple1)tuple).f0 ;

					// 获取窗口开始时间和结束时间
					long start = window.getStart();
					long end = window.getEnd();

					// 输出内容
					String output = "window[" + format.format(start) + "~" + format.format(end) + "] -> " + key ;

					// 对窗口中的数据进行聚合
					int count = 0 ;
					for (Tuple2<String, Integer> item: input){
						count += item.f1 ;
					}

					// 最后输出
					out.collect(Tuple2.of(output, count));
				}
			});

		// 4. 数据终端-sink
		countDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(_09StreamEventTimeWindowApply.class.getSimpleName()) ;
	}

}
