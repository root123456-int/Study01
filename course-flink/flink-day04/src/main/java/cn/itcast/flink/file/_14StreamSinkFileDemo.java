package cn.itcast.flink.file;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * Flink Stream 流计算，将DataStream 保存至文件系统，使用FileSystem Connector
 */
public class _14StreamSinkFileDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStreamSource<String> lineDataStream = env.socketTextStream("node1", 9999);

		// 3. 数据转换-transformation
		// 4. 数据终端-sink

		StreamingFileSink.RowFormatBuilder<String, String, ? extends StreamingFileSink.RowFormatBuilder<String, String, ?>> sink = StreamingFileSink
				//设置输出数据格式:要么是Row,要么是Bulk
				.forRowFormat(
						new Path("datas/output/stream-sink/"),
						new SimpleStringEncoder<String>("UTF-8")
				)
				//设置文件滚动策略
				.withRollingPolicy(
						DefaultRollingPolicy.builder()
								.withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
								.withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
								.withMaxPartSize(1024 * 1024 * 1024)
								.build()
				)
				.withOutputFileConfig(
						new OutputFileConfig("itcast", "flink")
				);

		//添加Sink
		lineDataStream.addSink(sink.build());


		// 5. 触发执行
		env.execute(_14StreamSinkFileDemo.class.getSimpleName());
	}

}
