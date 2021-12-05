package cn.itcast.flink.sink.mysql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 案例演示：自定义Sink，将数据保存至MySQL表中，继承RichSinkFunction
 */
public class _16StreamSinkJdbcDemo {

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class Student{
		private Integer id ;
		private String name ;
		private Integer age ;
	}


	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<Student> inputDataStream = env.fromElements(
			new Student(23, "wangwu2", 20),
			new Student(24, "zhaoliu2", 19),
			new Student(25, "wangwu2", 20),
			new Student(26, "zhaoliu2", 19)
		);

		// 3. 数据转换-transformation
		// 4. 数据终端-sink
		// TODO: 创建JdbcSink对象


		// 设置JdbcSink对象


		// 5. 触发执行-execute
		env.execute(_16StreamSinkJdbcDemo.class.getSimpleName()) ;
	}


}
