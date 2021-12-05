package cn.itcast.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author by FLX
 * @date 2021/7/16 0016 23:01.
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.准备数据-source
        DataSource<String> inputDataSet = env.readTextFile("datas/wordcount.data");
        //3.处理数据-transformation

        /*
			step1. 将每行数据按照分割符分割为单词
				spark spark flink  ->  spark, spark, flink
			step2. 将每个单词转换为二元组，表示每个单词出现一次
				spark, spark, flink ->  (spark, 1),  (spark, 1), (flink, 1)
			step3. 按照单词分组，将同组中次数进行累加sum
				(spark, 1),  (spark, 1), (flink, 1) -> (spark, 1 + 1 = 2) , (flink, 1 = 1)
		 */
        // step1. 将每行数据按照分割符分割为单词
        FlatMapOperator<String, String> wordDataSet = inputDataSet.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.trim().split("\\s+");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        // step2. 将每个单词转换为二元组，表示每个单词出现一次
        MapOperator<String, Tuple2<String, Integer>> tupleDataSet = wordDataSet.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        // 按照单词分组，组内求和
		/*
			flink spark flink  ->  .... -> (flink, 1)  (spark, 1)  (flink, 1)
													|groupBy(0) 按照二元组第1个下标分组
										flink -> [(flink, 1)  (flink, 1)]     spark -> [(spark, 1)]
													|
										flink -> 1 + 1 + 1 = 3        spark -> 1
		 */
        AggregateOperator<Tuple2<String, Integer>> resultDataSet = tupleDataSet
                //分组
                .groupBy(0)
                //组内求和
                .sum(1);

        //4.输出结果-sink
        resultDataSet.print();
        //5.触发执行-execute TODO:批处理,不需要触发执行
    }
}
