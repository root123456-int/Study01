package cn.itcast.flink.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

/**
 * DataSet API 批处理中数据源：基于集合Source
 * 1.env.fromElements(可变参数);
 * 2.env.fromColletion(各种集合);
 * 3.env.generateSequence(开始,结束);
 */
public class _03BatchSourceCollectionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 数据源-source
        // 方式一：可变参数
        DataSource<String> DS01 = env.fromElements("spark", "hadoop", "flink");
        DS01.printToErr();

        // 方式二：Java集合对象
        DataSource<String> DS02 = env.fromCollection(Arrays.asList("spark", "hadoop", "flink"));
        DS02.print();

        // 方式三：自动生成序列（整形）
        DataSource<Long> DS03 = env.generateSequence(1, 20);
        DS03.printToErr();
    }

}
