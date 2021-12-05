package cn.itcast.flink.window.time;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口统计案例演示：滑动时间窗口（Sliding Time Window），实时交通卡口车流量统计
 */
public class _03StreamSlidingTimeWindow {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1", 9999);

        // 3. 数据转换-transformation
/*
数据：
a,3
a,2
a,7
d,9
b,6
a,5
b,3
e,7
e,4
 */


        // TODO: 先按照卡口分组，再进行窗口操作，最后聚合累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDStream = inputDataStream
                .filter(line -> null != line && line.trim().split(",").length == 2)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String line) throws Exception {
                        String[] array = line.trim().split(",");

                        return new Tuple2<>(array[0], Integer.parseInt(array[1]));
                    }
                });


        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDStream = mapDStream
                .keyBy(0)
                .timeWindow(Time.seconds(4), Time.seconds(2))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {

                    private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void apply(Tuple tuple,
                                      TimeWindow window,
                                      Iterable<Tuple2<String, Integer>> input,
                                      Collector<Tuple2<String, Integer>> out) throws Exception {

                        String key = (String) ((Tuple1) (tuple)).f0;

                        long start = window.getStart();
                        long end = window.getEnd();

                        String output = "[" + format.format(start) + "~" + format.format(end) + "] -> " + key;

                        int count = 0;
                        for (Tuple2<String, Integer> item : input) {
                            count += item.f1;
                        }

                        out.collect(Tuple2.of(output, count));
                    }
                });


        // 4. 数据终端-sink
        resultDStream.printToErr();

        // 5. 触发执行-execute
        env.execute(_03StreamSlidingTimeWindow.class.getSimpleName());
    }

}
