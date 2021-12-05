package cn.itcast.flink.exactly;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
  * 1. Kafka Producer的容错-Kafka 0.9 and 0.10
  * 如果Flink开启了checkpoint，针对FlinkKafkaProducer09 和FlinkKafkaProducer010 可以提供 at-least-once的语义，还需要配置如下参数
      * setLogFailuresOnly(false)
      * setFlushOnCheckpoint(true)
  * retries【这个参数的值默认是0】 //注意：建议修改kafka 生产者的重试次数 
  *
  * 2. Kafka Producer的容错-Kafka 0.11+
  * 如果Flink开启了checkpoint，针对FlinkKafkaProducer011+ 就可以提供 exactly-once的语义
  * 但是需要选择具体的语义
      * Semantic.NONE
      * Semantic.AT_LEAST_ONCE【默认】
      * Semantic.EXACTLY_ONCE
  */
public class _06StreamExactlyOnceKafkaDemo {

	/*
/export/server/kafka/bin/kafka-topics.sh --list --bootstrap-server node1.itcast.cn:9092
    Source Kafka:
/export/server/kafka/bin/kafka-topics.sh --create \
--bootstrap-server node1.itcast.cn:9092 --replication-factor 1 --partitions 3 --topic flink-topic-source
/export/server/kafka/bin/kafka-console-producer.sh --broker-list node1.itcast.cn:9092 \
--topic flink-topic-source
    Sink Kafka:
/export/server/kafka/bin/kafka-topics.sh --create --bootstrap-server node1.itcast.cn:9092 \
--replication-factor 1 --partitions 3 --topic flink-topic-sink
/export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1.itcast.cn:9092 \
--topic flink-topic-sink --from-beginning
     */
	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);
		// 设置Checkpoint
		env.enableCheckpointing(5000);
		//env.setStateBackend(new FsStateBackend("file:///D:/datas/ckpt"));
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
			"flink-topic-source", new SimpleStringSchema(), sourceProps
		);
		// 从group offset记录的位置位置开始消费,如果没有该group信息，会根据"auto.offset.reset"的设置来决定从哪开始消费
		kafkaSource.setStartFromGroupOffsets();
		// Flink执行Checkpoint的时候提交偏移量(一份在Checkpoint中, 一份在Kafka主题中__comsumer_offsets(方便外部监控工具去看))
		kafkaSource.setCommitOffsetsOnCheckpoints(true) ;

		// 2.3. 添加数据源
		DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);

		// 3. 数据转换-transformation

		// 4. 数据终端-sink
		// 4.1. Kafka Producer 生成者属性配置
		Properties sinkProps = new Properties();
		sinkProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
		//设置事务超时时间，也可在kafka配置中设置
		sinkProps.setProperty("transaction.timeout.ms", 60000 * 15 + "");
		// 4.2. 创建序列化实例对象
		KafkaSerializationSchema<String> kafkaSchema = new KafkaSerializationSchema<String>(){
			@Override
			public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
				ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
					"flink-topic-sink", element.getBytes(StandardCharsets.UTF_8)
				);
				return record;
			}
		} ;
		// 4.3. 实例化FlinkKafkaProducer对象
		FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
			"flink-topic-sink",
			kafkaSchema,
			sinkProps,
			FlinkKafkaProducer.Semantic.EXACTLY_ONCE
		);
		// 4.4. 添加sink
		kafkaDataStream.addSink(kafkaSink) ;

		// 5. 触发执行-execute
		env.execute(_06StreamExactlyOnceKafkaDemo.class.getSimpleName());
	}

}
