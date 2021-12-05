package cn.itcast.flink.transformation;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink中批处理DataSet 转换函数基本使用
 */
public class _06BasicFunctionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataSource<String> inputDS = env.readTextFile("datas/click.log");

        // 3. 数据转换-transformation
        // TODO: 函数一【map函数】，将JSON转换为JavaBean对象

        MapOperator<String, ClickLog> clickLogDS = inputDS.map(new MapFunction<String, ClickLog>() {
            @Override
            public ClickLog map(String value) throws Exception {
                //使用FastJson库转换字符串为实体类对象
                return JSON.parseObject(value, ClickLog.class);
            }
        });
        //clickLogDS.first(10).printToErr();


        // TODO: 函数二【flatMap】，每条数据转换为日期时间格式

        FlatMapOperator<ClickLog, String> flatMapDS = clickLogDS.flatMap(new FlatMapFunction<ClickLog, String>() {
            @Override
            public void flatMap(ClickLog clickLog, Collector<String> out) throws Exception {
                //获取日期时间字段值
                // 1577890860000 -> Long类型
                Long entryTime = clickLog.getEntryTime();

                // 年-月-日-时： yyyy-MM-dd-HH
                String hour = DateFormatUtils.format(entryTime, "yyyy-MM-dd-HH");
                out.collect(hour);

                // 年-月-日： yyyy-MM-dd
                String day = DateFormatUtils.format(entryTime, "yyyy-MM-dd");
                out.collect(day);

                // 年-月： yyyy-MM
                String month = DateFormatUtils.format(entryTime, "yyyy-MM");
                out.collect(month);
            }
        });
        //flatMapDS.first(10).printToErr();

        // TODO: 函数三【filter函数】，过滤使用谷歌浏览器数据

        FilterOperator<ClickLog> filterDS = clickLogDS.filter(new FilterFunction<ClickLog>() {
            @Override
            public boolean filter(ClickLog clickLog) throws Exception {

                return "谷歌浏览器".equals(clickLog.getBrowserType());
            }
        });
        //filterDS.first(10).printToErr();


        // TODO：函数四【groupBy】函数与【sum】函数，按照浏览器类型分组，统计次数

        AggregateOperator<Tuple2<String, Integer>> sumDS = clickLogDS.map(new MapFunction<ClickLog, Tuple2<String, Integer>>() {
            //对ClickLog按照浏览器类型分组，统计每个浏览器访问次数
            @Override
            public Tuple2<String, Integer> map(ClickLog clickLog) throws Exception {

                return Tuple2.of(clickLog.getBrowserType(), 1);
            }
        })
                .groupBy(0)
                .sum(1);
        //sumDS.printToErr();

        /*
        (360浏览器,23)
        (qq浏览器,29)
        (火狐,24)
        (谷歌浏览器,24)
         */

        // TODO: 函数五【min和minBy】函数使用，求取最小值
        AggregateOperator<Tuple2<String, Integer>> minDS = sumDS.min(1);
        //minDS.printToErr();
        //(谷歌浏览器,23)

        ReduceOperator<Tuple2<String, Integer>> minByDS = sumDS.minBy(1);
        //minByDS.print();
        //(360浏览器,23)


        // TODO: 函数六【aggregate】函数，对数据进行聚合操作，需要指定聚合函数和字段

        AggregateOperator<Tuple2<String, Integer>> sumAggDataSet = sumDS.aggregate(Aggregations.SUM, 1);
        //sumAggDataSet.printToErr();
        /*
        (谷歌浏览器,100)
         */

        AggregateOperator<Tuple2<String, Integer>> maxAggDataSet = sumDS.aggregate(Aggregations.MAX, 1);
        //maxAggDataSet.print();
        /*
        (谷歌浏览器,29)
         */


        // TODO： 函数七【reduce和reduceGroup】使用,统计每个浏览器访问次数
        UnsortedGrouping<Tuple2<String, Integer>> group = clickLogDS
                .map(new MapFunction<ClickLog, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(ClickLog clickLog) throws Exception {
                        return Tuple2.of(clickLog.getBrowserType(), 1);
                    }
                })
                //先按照浏览器分组
                .groupBy(0);

        //TODO:使用reduce函数聚合
        ReduceOperator<Tuple2<String, Integer>> reduceDataSet = group.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tmp, Tuple2<String, Integer> item) throws Exception {

                //1. 计算次数
                Integer count = tmp.f1 + item.f1;
                return Tuple2.of(tmp.f0, count);
            }
        });
        //reduceDataSet.print();

        //System.out.println("======================");

        //TODO:使用reduceGroup函数聚合

        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> groupReduceDataSet = group.reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, Integer>> values, //分组后,组内的所有元素
                               Collector<Tuple2<String, Integer>> out) throws Exception {

                String key = "";
                Integer count = 0;

                for (Tuple2<String, Integer> value : values) {
                    key = value.f0;

                    count += value.f1;
                }
                //输出数据
                out.collect(Tuple2.of(key, count));

            }
        });
        //groupReduceDataSet.printToErr();


        // TODO: 函数八【union函数】，合并数据类型相同2个数据集

        MapOperator<String, ClickLog> dataSet01 = env.readTextFile("datas/input/click1.log")
                .map(new MapFunction<String, ClickLog>() {
                    @Override
                    public ClickLog map(String line) throws Exception {
                        return JSON.parseObject(line, ClickLog.class);
                    }
                });

        //System.out.println("DataSet01 条目数：" + dataSet01.count());

        MapOperator<String, ClickLog> dataSet02 = env.readTextFile("datas/input/click2.log")
                .map(new MapFunction<String, ClickLog>() {
                    @Override
                    public ClickLog map(String line) throws Exception {

                        return JSON.parseObject(line, ClickLog.class);
                    }
                });
        //System.out.println("DataSet02 条目数：" + dataSet02.count());

        //使用Union函数进行合并
        UnionOperator<ClickLog> unionDataSet = dataSet01.union(dataSet02);
        //System.out.println("unionDataSet 条目数：" + unionDataSet.count());

        // TODO: 函数九【distinct函数】对数据进行去重操作
        DistinctOperator<ClickLog> distinctDataSet = clickLogDS.distinct("browserType");

        distinctDataSet.printToErr();
    }
}
