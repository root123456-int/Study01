package cn.itcast.flink.start;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 基于Flink引擎实现批处理词频统计WordCount：过滤filter、排序sort等操作
 */
public class _01WordCount {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2. 数据源-source
        DataSource<String> lineDataSet = env.readTextFile("datas/wordcount.data");

        // 3. 数据转换-transformation
        AggregateOperator<Tuple2<String, Integer>> sumDataSet = lineDataSet
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {

                        return null != line && line.trim().length() > 0;
                    }
                })
                //单词分割,出现1次,返回二元组
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : line.trim().split("\\W+")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                //按照单词分组,进行词频累加sum求和
                .groupBy(0)
                .sum(1);

        // 4. 数据终端-sink
		/*
			(flink,1)
			(hbase,1)
			(hadoop,3)
			(hive,3)
			(spark,7)
		 */
        //sumDataSet.print();

        // 对词频统计结果，按照count进行降序排序,
        // TODO: 对每个分区数据排序，并不是全局排序，如果要实现全局排序，需要设置并行度为1
        SortPartitionOperator<Tuple2<String, Integer>> sortDataSet = sumDataSet
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1);

        sortDataSet.printToErr();

        // 5. 触发执行-execute

    }

}
