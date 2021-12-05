package cn.itcast.flink.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink State 中KeyedState，默认情况下框架自己维护，此外可以手动维护
 */
public class _02StreamKeyedStateDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<Tuple3<String, String, Long>> tupleStream = env.fromElements(
			Tuple3.of("上海", "普陀区", 488L), Tuple3.of("上海", "徐汇区", 212L),
			Tuple3.of("北京", "西城区", 823L), Tuple3.of("北京", "海淀区", 234L),
			Tuple3.of("上海", "杨浦区", 888L), Tuple3.of("上海", "浦东新区", 666L),
			Tuple3.of("北京", "东城区", 323L), Tuple3.of("上海", "黄浦区", 111L)
		);

		// 3. 数据转换-transformation
		// TODO：使用DataStream转换函数max获取每个市最大值
		SingleOutputStreamOperator<Tuple3<String, String, Long>> maxDataStream = tupleStream
			.keyBy(0)
			.max(2);
		// 4. 数据终端-sink
		/*
			(上海,普陀区,488)
			(上海,普陀区,488)
			(北京,西城区,823)
			(北京,西城区,823)
			(上海,杨浦区,888)
			(上海,杨浦区,888)
			(北京,西城区,823)
			(上海,杨浦区,888)
		 */
		//maxDataStream.printToErr();

		// TODO: 自己管理KededState存储每个Key状态State
		SingleOutputStreamOperator<String> stateDataStream = tupleStream
			.keyBy(0) // 城市city分组
			.map(new RichMapFunction<Tuple3<String, String, Long>, String>() {
				// TODO: step1. 定义存储状态数据结构
				// transient是类型修饰符，只能用来修饰字段。在对象序列化的过程中，标记为transient的变量不会被序列化
				private transient ValueState<Long> valueState = null ;

				@Override
				public void open(Configuration parameters) throws Exception {
					// TODO: step2. 初始化状态的值
					valueState = getRuntimeContext().getState(
						new ValueStateDescriptor<Long>("maxState", Long.class)
					);
				}

				@Override
				public String map(Tuple3<String, String, Long> tuple) throws Exception {
					/*
						Key: 上海，  tuple： 上海,普陀区,488
					 */
					// TODO: step3. 从以前状态中获取值
					Long historyValue = valueState.value();

					// 获取当前key中的值
					Long currentValue = tuple.f2 ;

					// 判断历史值是否为null，如果key的数据第1次出现，以前没有状态
					if(null == historyValue || currentValue > historyValue){
						// TODO: step4. 更新状态值
						valueState.update(currentValue);
					}

					// 返回处理结果
					return tuple.f0 + ", " + valueState.value();
				}
			});
		/*
			上海, 488
			上海, 488
			北京, 823
			北京, 823
			上海, 888
			上海, 888
			北京, 823
			上海, 888
		 */
		stateDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(_02StreamKeyedStateDemo.class.getSimpleName()) ;
	}

}
