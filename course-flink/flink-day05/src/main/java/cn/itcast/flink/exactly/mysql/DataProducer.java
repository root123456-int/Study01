package cn.itcast.flink.exactly.mysql;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class DataProducer {

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class DataBean {
		private String value;
	}

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "node1.itcast.cn:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(props);

		try {
			for (int i = 1; i <= 20; i++) {
				DataBean data = new DataBean(String.valueOf(i));
				ProducerRecord record = new ProducerRecord<String, String>(
					"flink-topic", JSON.toJSONString(data)
				);
				producer.send(record);
				System.out.println("发送数据: " + JSON.toJSONString(data));
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			System.out.println(e);
		}

		producer.flush();
	}
}
