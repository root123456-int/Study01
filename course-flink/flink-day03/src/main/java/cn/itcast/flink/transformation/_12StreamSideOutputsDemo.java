package cn.itcast.flink.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 流计算中转换函数：使用侧边流SideOutputs
 */
public class _12StreamSideOutputsDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<Long> dataStream = env.generateSequence(1, 10);

		// 3. 数据转换-transformation


		// 打印本身流的数据


		// 获取侧边流，进行打印


		// 4. 数据终端-sink

		// 5. 触发执行-execute
		env.execute(_12StreamSideOutputsDemo.class.getSimpleName()) ;
	}

}
