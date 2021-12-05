package cn.itcast.flink.watermark;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源，实时产生订单数据，继承RichParallelSourceFunction接口
 */
public class OrderSource extends RichParallelSourceFunction<Order> {

	// 标识符：表示是否运行产生数据
	private boolean isRunning = true;

	// 不断执行，实时产生数据
	@Override
	public void run(SourceContext<Order> ctx) throws Exception {

		int[] times = new int[]{0, 0, 8, 5, 10, 18, 15, 12};
		// 随机实例对象
		Random random = new Random();
		while (isRunning) {
			// 创建订单对象
			Order order = new Order(
				UUID.randomUUID().toString().substring(1, 18), //
				"u100" + random.nextInt(2), // u1000, u1001
				random.nextInt(100) + 1, //
				// 为了演示生成的订单数据乱序达到Flink应用，当获取当前时间戳以后，再减去随机时间0,1,2,3,4
				System.currentTimeMillis() - times[random.nextInt(times.length)] * 1000//
			);
			System.out.println("order>>>" + order);
			// 发送数据
			ctx.collect(order);
			// TODO: 每秒钟产生一条数据
			TimeUnit.SECONDS.sleep(2);
		}
	}

	@Override
	public void cancel() {
		// 当不在接收数据时，设置isRunning为false
		isRunning = false;
	}
}
