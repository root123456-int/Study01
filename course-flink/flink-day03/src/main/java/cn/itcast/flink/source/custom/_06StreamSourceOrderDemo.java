package cn.itcast.flink.source.custom;

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
 * 每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
 * - 随机生成订单ID：UUID
 * - 随机生成用户ID：0-2
 * - 随机生成订单金额：0-100
 * - 时间戳为当前系统时间：current_timestamp
 */
public class _06StreamSourceOrderDemo {

    /*
    自定义数据源,每隔1秒产生交易订单数据
     */
    private static class OrderSource extends RichParallelSourceFunction<Order> {

        //定义标识变量,表示是否产生数据
        private boolean isRunning = true;

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            //随机数Random对象
            Random random = new Random();

            //需要产生数据时,模拟产生数据,并且发送
            while (isRunning) {
                //模拟产生交易订单数据
                Order order = new Order(
                        UUID.randomUUID().toString(),
                        random.nextInt(2) + 1,
                        random.nextDouble() * 100,
                        System.currentTimeMillis()
                );

                //发送数据
                ctx.collect(order);

                //休眠1秒
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            //当取消运行Job时,不再产生数据
            isRunning = false;
        }
    }

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

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Order> OrderDStream = env.addSource(new OrderSource());


        // 3. 数据转换-transformation
        // 4. 数据终端-sink
        OrderDStream.printToErr();

        // 5. 触发执行-execute
        env.execute(_06StreamSourceOrderDemo.class.getSimpleName());
    }

}
