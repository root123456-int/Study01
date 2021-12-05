package cn.itcast.flink.start;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 基于 Flink 流计算引擎：从TCP Socket消费数据，实时词频统计WordCount
 */
public class _01WindowWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1", 9999);

        // 3. 数据转换-transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleStream = inputDataStream
                // a. 过滤数据，尤其null和空字符串
                .filter(line -> null != line && line.trim().length() > 0)
                // b. 将每行数据分割为单词
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> out) throws Exception {
                        for (String word : line.trim().toLowerCase().split("\\W+")) {
                            out.collect(word);
                        }
                    }
                })
                // c. 单词转换为二元组，表示每个单词出现一次
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                });

        // d. 按照单词分组，进行累加计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> timeWindowDStream = tupleStream
                //.keyBy(0).sum(1);
                // 4. 数据终端-sink
                //resultStream.printToErr() ;

                // TODO: KeyedStream窗口操作，先分组，再窗口，最后聚合
                .keyBy(0)
                //设置窗口,每5秒统计最近5s数据,滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //聚合操作
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple,
                                      TimeWindow window,
                                      Iterable<Tuple2<String, Integer>> input,
                                      Collector<Tuple2<String, Integer>> out) throws Exception {

                    }
                });

        // TODO: 未进行分组，直接窗口window，再聚合
        tupleStream
                .timeWindowAll(Time.seconds(10))
                .apply(new AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> values, Collector<Object> out) throws Exception {

                    }
                });


        // 5. 触发执行
        env.execute(_01WindowWordCount.class.getSimpleName());
    }

}
