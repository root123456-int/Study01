package cn.itcast.flink.sink.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 案例演示：将数据保存至Kafka Topic中，直接使用官方提供Connector
 *      /export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server node1.itcast.cn:9092 --topic flink-topic
 */
public class _17StreamSinkKafkaDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<Order> orderDataStream = env.addSource(new OrderSource());

		// 3. 数据转换-transformation：将Order订单对象转换JSON字符串


		// 4. 数据终端-sink


		// 5. 触发执行-execute
		env.execute(_17StreamSinkKafkaDemo.class.getSimpleName()) ;
	}

	/**
	 * 自定义实现Kafka Serialization Schema接口，实现将消息Message转换byte[]
	 */



	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Order {
		private String id;
		private Integer userId;
		private Double money;
		private Long orderTime;
	}

	/**
	 * 自定义数据源：每隔1秒产生1条交易订单数据
	 */
	private static class OrderSource extends RichParallelSourceFunction<Order> {
		// 定义标识变量，表示是否产生数据
		private boolean isRunning = true;

		// 模拟产生交易订单数据
		@Override
		public void run(SourceContext<Order> ctx) throws Exception {
			Random random = new Random() ;
			while (isRunning){
				// 构建交易订单数据
				Order order = new Order(
					UUID.randomUUID().toString().substring(0, 18), //
					random.nextInt(2) + 1 , //
					random.nextDouble() * 100 ,//
					System.currentTimeMillis()
				);

				// 将数据输出
				ctx.collect(order);

				// 每隔1秒产生1条数据，线程休眠
				TimeUnit.SECONDS.sleep(1);
			}
		}

		@Override
		public void cancel() {
			isRunning = false ;
		}
	}

}
