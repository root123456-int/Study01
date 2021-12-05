package cn.itcast.flink.source.basic;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Flink 流计算数据源：基于集合的Source，分别为可变参数、集合和自动生成数据
 */
public class _04StreamSourceCollectionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        // 可变参数
        DataStreamSource<String> DStream01 = env.fromElements("spark", "flink", "hive");
        DStream01.printToErr();

        // 集合对象
        DataStreamSource<String> DStream02 = env.fromCollection(Arrays.asList("spark", "flink", "hive"));
        DStream02.print();

        // 自动生成序列数字
        DataStreamSource<Long> DStream03 = env.generateSequence(1, 10);
        DStream03.printToErr();

        // 5. 触发执行-execute
        env.execute(_04StreamSourceCollectionDemo.class.getSimpleName());
    }

}
