package cn.itcast.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Flink 批处理DataSet API中分区函数
 * TODO: rebalance、partitionBy*
 */
public class _08BatchRepartitionDemo {

    public static void main(String[] args) throws Exception {

        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 2. 数据源-source
/*
        DataSource<Long> ds01 = env.generateSequence(1, 30);
        ds01.map(new RichMapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return getRuntimeContext().getIndexOfThisSubtask() + ": " + value;
            }
        }).printToErr();

 */

        // TODO: 将各个分区数据不均衡，过滤
/*
        FilterOperator<Long> filterDS = ds01.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {

                return value > 5 && value < 29;

            }
        });
        filterDS.map(new RichMapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return getRuntimeContext().getIndexOfThisSubtask() + ":" + value;
            }
        }).print();

 */

        //System.out.println("======================");
        // TODO: 使用rebalance函数，对DataSet数据集数据进行重平衡
        /*
        filterDS
                .rebalance()
                .map(new RichMapFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {

                        return getRuntimeContext().getIndexOfThisSubtask() + ": " + value;
                    }
                }).printToErr();

         */


        // ====================================================================
        // TODO: 其他一些分区函数，针对数据类型为Tuple元组
        List<Tuple3<Integer, Long, String>> list = new ArrayList<>();
        list.add(Tuple3.of(1, 1L, "Hello"));
        list.add(Tuple3.of(2, 2L, "Hello"));
        list.add(Tuple3.of(3, 2L, "Hello"));
        list.add(Tuple3.of(4, 3L, "Hello"));
        list.add(Tuple3.of(5, 3L, "Hello"));
        list.add(Tuple3.of(6, 3L, "hehe"));
        list.add(Tuple3.of(7, 4L, "hehe"));
        list.add(Tuple3.of(8, 4L, "hehe"));
        list.add(Tuple3.of(9, 4L, "hehe"));
        list.add(Tuple3.of(10, 4L, "hehe"));
        list.add(Tuple3.of(11, 5L, "hehe"));
        list.add(Tuple3.of(12, 5L, "hehe"));
        list.add(Tuple3.of(13, 5L, "hehe"));
        list.add(Tuple3.of(14, 5L, "hehe"));
        list.add(Tuple3.of(15, 5L, "hehe"));
        list.add(Tuple3.of(16, 6L, "hehe"));
        list.add(Tuple3.of(17, 6L, "hehe"));
        list.add(Tuple3.of(18, 6L, "hehe"));
        list.add(Tuple3.of(19, 6L, "hehe"));
        list.add(Tuple3.of(20, 6L, "hehe"));
        list.add(Tuple3.of(21, 6L, "hehe"));
        // 将数据打乱，进行洗牌
        Collections.shuffle(list);

        DataSource<Tuple3<Integer, Long, String>> collectionDS = env.fromCollection(list);
        env.setParallelism(2);

        // TODO: a. 指定字段，按照hash进行分区
        collectionDS
                .partitionByHash(2)
                .map(new RichMapFunction<Tuple3<Integer, Long, String>, String>() {
                    @Override
                    public String map(Tuple3<Integer, Long, String> value) throws Exception {
                        int index = getRuntimeContext().getIndexOfThisSubtask();
                        return index + ": " + value.toString();
                    }
                }).print();
        System.out.println("==============================");

        // TODO: b. 指定字段，按照Range范围进行分区

        collectionDS
                .partitionByRange(1)
                .map(new RichMapFunction<Tuple3<Integer, Long, String>, String>() {
                    @Override
                    public String map(Tuple3<Integer, Long, String> value) throws Exception {

                        int index = getRuntimeContext().getIndexOfThisSubtask();
                        return index + ": " + value.toString();
                    }
                }).printToErr();

        System.out.println("====================");
        // TODO: c. 自定义分区规则
        collectionDS.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % 2;

            }
        }, 0)
                .map(new RichMapFunction<Tuple3<Integer, Long, String>, String>() {
                    @Override
                    public String map(Tuple3<Integer, Long, String> value) throws Exception {

                        int index = getRuntimeContext().getIndexOfThisSubtask();
                        return index + " : " + value.toString();
                    }
                }).print();


    }

}
