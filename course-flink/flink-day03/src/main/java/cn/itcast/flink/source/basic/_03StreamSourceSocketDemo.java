package cn.itcast.flink.source.basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 基于 Flink 流计算引擎：从TCP Socket消费数据，实时词频统计WordCount
 * TODO: flatMap（直接二元组）和KeySelector选择器、reduce函数聚合
 */
public class _03StreamSourceSocketDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO:创建本地运行,支持WEB UI界面执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);

        // 2. 数据源-source

        DataStreamSource<String> inputDStream = env.socketTextStream("192.168.88.101", 9999);

        // 3. 数据转换-
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDStream = inputDStream.filter(new FilterFunction<String>() {
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
                .keyBy("f0")
                .sum("f1");


        // 4. 数据终端-sink

        resultDStream.printToErr();
        
        // 5. 触发执行
        env.execute(_03StreamSourceSocketDemo.class.getSimpleName());
    }

}
