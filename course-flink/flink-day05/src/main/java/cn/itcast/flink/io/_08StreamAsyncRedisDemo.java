package cn.itcast.flink.io;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 采用异步方式请求Redis数据库，此处使用 Async IO实现
 */
public class _08StreamAsyncRedisDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<String> dataStream = env.readTextFile("datas/ab.txt");

		// 3. 数据转换-transformation
		// TODO: 异步请求Redis，获取城市ID，依据城市名称，进行比对操作
		SingleOutputStreamOperator<String> resultDataStream = AsyncDataStream.unorderedWait(
			dataStream, //
			new AsyncRedisRequest(), //
			9000,
			TimeUnit.MILLISECONDS,  // 设置连接超时时间大小和单位
			1 // 容量
		);

		// 4. 数据终端-sink
		resultDataStream.printToErr() ;

		// 5. 触发执行-execute
		env.execute(_08StreamAsyncRedisDemo.class.getSimpleName()) ;
	}

}
