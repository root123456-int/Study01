package cn.itcast.flink.exactly.mysql;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

public class _07StreamExactlyOnceMySQLDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 设置Checkpoint
		env.enableCheckpointing(5000);
		env.setStateBackend(new FsStateBackend("file:///D:/datas/ckpt"));
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		env.getCheckpointConfig().enableExternalizedCheckpoints(
			CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
		);

		// 2. 数据源-source
		// 2.1. Kafka Consumer 消费数据属性设置
		Properties sourceProps = new Properties();
		sourceProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "node1.itcast.cn:9092");
		sourceProps.setProperty("group.id","flink-1001");
		//如果有记录偏移量从记录的位置开始消费,如果没有从最新的数据开始消费
		sourceProps.setProperty("auto.offset.reset","latest");
		//开一个后台线程每隔5s检查Kafka的分区状态
		sourceProps.setProperty("flink.partition-discovery.interval-millis","5000");

		// 2.2. 实例化FlinkKafkaConsumer对象
		FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
			"flink-topic", new SimpleStringSchema(), sourceProps
		);
		// 从group offset记录的位置位置开始消费,如果没有该group信息，会根据"auto.offset.reset"的设置来决定从哪开始消费
		kafkaSource.setStartFromGroupOffsets();
		// Flink执行Checkpoint的时候提交偏移量(一份在Checkpoint中, 一份在Kafka主题中__comsumer_offsets(方便外部监控工具去看))
		kafkaSource.setCommitOffsetsOnCheckpoints(true) ;

		// 2.3. 添加数据源
		DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);

		//3. 数据转换-transformation
		//4.数据终端-Sink
		kafkaDataStream.addSink(new MySQLTwoPhaseCommitSink());

		//5.execute
		env.execute(_07StreamExactlyOnceMySQLDemo.class.getName());
	}

}
