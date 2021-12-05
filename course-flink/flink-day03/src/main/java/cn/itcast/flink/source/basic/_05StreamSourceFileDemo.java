package cn.itcast.flink.source.basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 流计算数据源：基于文件的Source
 */
public class _05StreamSourceFileDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputDStream = env.readTextFile("datas/wordcount.data");

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDStream = inputDStream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {

                        return null != line && line.trim().length() > 0;
                    }
                })
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : line.trim().split("\\W+")) {
                            out.collect(Tuple2.of(word, 1));

                        }
                    }
                })
                .keyBy(0)
                .sum(1);

        resultDStream.printToErr();
        // 5. 触发执行-execute
        env.execute(_05StreamSourceFileDemo.class.getSimpleName());
    }

}
