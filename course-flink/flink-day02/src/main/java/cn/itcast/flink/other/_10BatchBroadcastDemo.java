package cn.itcast.flink.other;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 批处理中广播变量：将小数据集广播至TaskManager内存中，便于使用
 */
public class _10BatchBroadcastDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source：从本地集合构建2个DataSet
        DataSource<Tuple2<Integer, String>> studentDataSet = env.fromCollection(
                Arrays.asList(Tuple2.of(1, "张三"), Tuple2.of(2, "李四"), Tuple2.of(3, "王五"))
        );
        DataSource<Tuple3<Integer, String, Integer>> scoreDataSet = env.fromCollection(
                Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英语", 86))
        );

        // 3. 数据转换-transform：使用map函数，定义加强映射函数RichMapFunction，使用广播变量值
		/*
			(1, "语文", 50) -> "张三" , "语文", 50
			(2, "数学", 70) -> "李四", "数学", 70
			(3, "英语", 86) -> "王五", "英语", 86
		*/
        MapOperator<Tuple3<Integer, String, Integer>, String> dataSet = scoreDataSet
                .map(new RichMapFunction<Tuple3<Integer, String, Integer>, String>() {
                    // 定义Map集合，存储小表的数据：学生数据 -> key: id, value: name
                    Map<Integer, String> stuMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        // TODO: step2. 调用map方法之前，从TaskManager中获取广播的数据,相当于初始化操作方法
                        List<Tuple2<Integer, String>> list = getRuntimeContext().getBroadcastVariable("students");

                        //TODO: step3. 使用广播变量的值
                        for (Tuple2<Integer, String> item : list) {
                            stuMap.put(item.f0, item.f1);
                        }
                    }

                    @Override
                    public String map(Tuple3<Integer, String, Integer> score) throws Exception {

                        //依据学生id获取学生姓名
                        Integer studentID = score.f0;
                        //根据studentID获取学生姓名,没有就未知
                        String stuname = stuMap.getOrDefault(studentID, "未知");
                        return stuname + ", " + score.f1 + ", " + score.f2;
                    }
                })
                //TODO:Step1,将小表数据广播出去,哪个函数使用数据集,就在哪个函数后面广播数据集
                .withBroadcastSet(studentDataSet, "students");

        // 4. 数据终端-sink
        dataSet.printToErr();

    }

}
