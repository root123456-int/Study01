package cn.itcast.flink.start;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 基于 Flink 流计算引擎：从TCP Socket消费数据，实时词频统计WordCount
 */
public class _01StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2. 数据源-source
        DataStreamSource<String> socketDataStream = env.socketTextStream("192.168.88.101", 9999);

        // 3. 数据转换-transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDStream = socketDataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {

                return null != line && line.trim().length() > 0;
            }
        })
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : line.trim().split("\\s+")) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);

        // 4. 数据终端-sink

        resultDStream.printToErr();

        // 5. 触发执行-execute
        env.execute(_01StreamWordCount.class.getSimpleName());
    }

}
