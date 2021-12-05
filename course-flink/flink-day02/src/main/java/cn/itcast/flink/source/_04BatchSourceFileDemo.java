package cn.itcast.flink.source;

import lombok.Data;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


/**
 * DataSet API 批处理中数据源：基于文件Source
 * 1.env.readTextFile(本地文件/HDFS文件); //压缩文件也可以
 * 2.env.readCsvFile[泛型]("本地文件/HDFS文件")
 * 3.env.readTextFile("目录").withParameters(parameters);
 * Configuration parameters = new Configuration();
 * parameters.setBoolean("recursive.file.enumeration", true);//设置是否递归读取目录
 */
public class _04BatchSourceFileDemo {

    @Data
    public static class Rating {
        public Integer userId;
        public Integer movieId;
        public Double rating;
        public Long timestamp;
    }


    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 数据源-source
        // 方式一：读取文件，文本文件，可以是压缩
        DataSource<String> gzDS01 = env.readTextFile("datas/wordcount.data.gz");
        //gzDS01.print();

        AggregateOperator<Tuple2<String, Integer>> resultDS = gzDS01.filter(new FilterFunction<String>() {
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
                .groupBy(0)
                .sum(1);

        resultDS.printToErr();



        // 方式二：读取CSV文件
        DataSource<Rating> ratingDataSource = env.readCsvFile("datas/u.data")
                //设置分隔符
                .fieldDelimiter("\t")
                //转换为POJO对象
                .pojoType(Rating.class, "userId", "movieId", "rating", "timestamp");

        ratingDataSource.print();

        // 方式三：递归读取子目录中文件数据
        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration",true);

        DataSource<String> recursiveDS = env.readTextFile("datas/subDatas")
                .withParameters(parameters);
        recursiveDS.printToErr();

    }

}
