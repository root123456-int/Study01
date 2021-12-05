package cn.itcast.flink.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 滚动事件时间窗口（Tumbling EventTime Window）统计：每隔5秒，计算5秒内，每个用户的订单金额
 * TODO：添加Watermark水位线，来解决一定程度上的数据延迟和数据乱序问题
 */
public class _11StreamEventTimeWatermarkWindow {

    public static void main(String[] args) throws Exception {

        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // TODO: step1. 设置时间语义：事件时间


        // 2. 数据源-source
        DataStreamSource<Order> orderStream = env.addSource(new OrderSource());

        // TODO: step2. 提取事件时间字段值，转换为Long类型，并且设置最大允许的延迟时间或乱序时间
        SingleOutputStreamOperator<Order> timeDStream = orderStream.assignTimestampsAndWatermarks(
                // TODO： 设置最大运行延迟时间，从而计算出每条数据Watermark水位线 = 事件时间 - 最大允许延迟时间
                new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(Order order) {
                        return order.getOrderTime();
                    }
                }
        );

        // 3. 数据转换-transformation: 每隔5秒，计算5秒内，每个用户的订单金额
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDStream = timeDStream
                .map(new MapFunction<Order, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Order order) throws Exception {
                        String userId = order.getUserId();
                        Integer money = order.getMoney();

                        return Tuple2.of(userId, money);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);


        // 4. 数据终端-sink
        resultDStream.printToErr();


        // 5. 触发执行-execute
        env.execute(_11StreamEventTimeWatermarkWindow.class.getSimpleName());
    }

}
