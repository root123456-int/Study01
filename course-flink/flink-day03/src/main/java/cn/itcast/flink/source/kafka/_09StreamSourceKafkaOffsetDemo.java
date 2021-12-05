package cn.itcast.flink.source.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink从Kafka消费数据，指定topic名称和反序列化类，可以指定消费数据开始偏移量
 */
public class _09StreamSourceKafkaOffsetDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;
		env.setParallelism(1) ; // 代码中设置全局并行度

		// 2. 数据源-source
		// 从Kafka消费数据时，设置参数值
		Properties props = new Properties() ;
		props.setProperty("bootstrap.servers", "node1.itcast.cn:9092");
		props.setProperty("group.id", "test-1001");
		// 传递参数，创建FlinkKafkaConsumer实例对象
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
			"flink-topic", //
			new SimpleStringSchema(), //
			props
		) ;
		// TODO: 1、Flink从topic中最初的数据开始消费
		//kafkaConsumer.setStartFromEarliest() ;

		// TODO: 2、Flink从topic中最新的数据开始消费
		//kafkaConsumer.setStartFromLatest();

		// TODO: 3、Flink从topic中指定的group上次消费的位置开始消费，所以必须配置group.id参数
		//kafkaConsumer.setStartFromGroupOffsets() ;

		// TODO: 4、Flink从topic中指定的offset开始，这个比较复杂，需要手动指定offset
		Map<KafkaTopicPartition, Long> offsets = new HashMap<>();
		offsets.put(new KafkaTopicPartition("flink-topic", 0), 1L);
		offsets.put(new KafkaTopicPartition("flink-topic", 1), 8L);
		offsets.put(new KafkaTopicPartition("flink-topic", 2), 5L);
		//kafkaConsumer.setStartFromSpecificOffsets(offsets);


		// 从Kafka消费数据
		DataStreamSource<String> kafkaDataStream = env.addSource(kafkaConsumer);

		// 3. 数据转换-transformation
		// 4. 数据终端-sink
		kafkaDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(_09StreamSourceKafkaOffsetDemo.class.getSimpleName()) ;
	}

}
