package cn.itcast.flink.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 流计算中转换函数：合并union和连接connect
 */
public class _10StreamUnionConnectDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<String> dataStream01 = env.fromElements("A", "B", "C", "D");
		DataStreamSource<String> dataStream02 = env.fromElements("aa", "bb", "cc", "dd");
		DataStreamSource<Integer> dataStream03 = env.fromElements(1, 2, 3, 4);

		// 3. 数据转换-transformation
		// TODO: 两个流进行union


		// TODO: 两个流进行连接


		// 4. 数据终端-sink

		// 5. 触发执行-execute
		env.execute(_10StreamUnionConnectDemo.class.getSimpleName()) ;
	}

}
