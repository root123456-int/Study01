package cn.itcast.flink.source.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Flink从Kafka消费数据，指定topic名称和反序列化类
 */
public class _08StreamSourceKafkaDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 代码中设置全局并行度

        // 2. 数据源-source
        //设置Kafka Consumer时连接信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092");
        props.put("group.id", "gid-1001");
        //a. 构建FlinkKafkaConsumer对象
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                "flink-topic",
                new SimpleStringSchema(),
                props
        );

        //b. 添加数据源Source
        DataStreamSource<String> kafkaDStream = env.addSource(kafkaConsumer);

        // 3. 数据转换-transformation
        // 4. 数据终端-sink
        kafkaDStream.printToErr();

        // 5. 触发执行-execute
        env.execute(_08StreamSourceKafkaDemo.class.getSimpleName());
    }

}
