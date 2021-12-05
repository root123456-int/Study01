package cn.itcast.flink.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 流计算中转换函数：split流的拆分和select流的选择
 */
public class _11StreamSplitSelectDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<Long> dataStream = env.generateSequence(1, 10);

		// 3. 数据转换-transformation
		// TODO: 按照奇数和偶数分割流


		// TODO: 使用select函数，依据名称获取分割流


		// 4. 数据终端-sink

		// 5. 触发执行-execute
		env.execute(_11StreamSplitSelectDemo.class.getSimpleName()) ;
	}

}
