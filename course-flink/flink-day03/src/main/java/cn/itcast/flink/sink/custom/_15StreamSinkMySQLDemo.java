package cn.itcast.flink.sink.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 案例演示：自定义Sink，将数据保存至MySQL表中，继承RichSinkFunction
 */
public class _15StreamSinkMySQLDemo {

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class Student{
		private Integer id ;
		private String name ;
		private Integer age ;
	}

	/**
	 * 自定义Sink，将数据保存至MySQL表中
	 */



	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<Student> inputDataStream = env.fromElements(
			new Student(13, "wangwu", 20),
			new Student(14, "zhaoliu", 19),
			new Student(15, "wangwu", 20),
			new Student(16, "zhaoliu", 19)
		);

		// 3. 数据转换-transformation
		// 4. 数据终端-sink

		// 5. 触发执行-execute
		env.execute(_15StreamSinkMySQLDemo.class.getSimpleName()) ;
	}


}
