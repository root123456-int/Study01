package cn.itcast.flink.other;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 批处理中分布式缓存：将小文件数据进行缓存
 */
public class _11BatchDistributedCacheDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO: step1. 将小文件数据进行缓存
        env.registerCachedFile("datas/distribute_cache_student", "student_cache");


        // 2. 数据源-source：从本地集合构建2个DataSet
        DataSource<Tuple3<Integer, String, Integer>> scoreDataSet = env.fromCollection(
                Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英语", 86))
        );

        // 3. 数据转换-transform：使用map函数，定义加强映射函数RichMapFunction，使用广播变量值
		/*
			(1, "语文", 50) -> "张三" , "语文", 50
			(2, "数学", 70) -> "李四", "数学", 70
			(3, "英语", 86) -> "王五", "英语", 86
		*/
        MapOperator<Tuple3<Integer, String, Integer>, String> dataset = scoreDataSet.map(new RichMapFunction<Tuple3<Integer, String, Integer>, String>() {

            Map<Integer, String> stuMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                // TODO: step2. 从分布式缓存中获取文件
                File cacheFlie = getRuntimeContext().getDistributedCache().getFile("student_cache");

                // TODO: step3. 读取缓存文件内容，解析存储到Map集合
                List<String> list = FileUtils.readLines(cacheFlie);
                for (String item : list) {
                    String[] strings = item.trim().split(",");
                    stuMap.put(Integer.valueOf(strings[0]), strings[1]);
                }
            }

            @Override
            public String map(Tuple3<Integer, String, Integer> score) throws Exception {

                Integer studentID = score.f0;

                String stuName = stuMap.getOrDefault(studentID, "未知");

                return stuName + ", " + score.f1 + ", " + score.f2;
            }
        });

        // 4. 数据终端-sink
        dataset.printToErr();

    }

}
