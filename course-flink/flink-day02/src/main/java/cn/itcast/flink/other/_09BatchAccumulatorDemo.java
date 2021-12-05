package cn.itcast.flink.other;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;


/**
 * 演示Flink中累加器Accumulator使用，统计处理的条目数
 */
public class _09BatchAccumulatorDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataSource<String> dataset = env.readTextFile("datas/click.log");

        // 3. 数据转换-transformation
        // TODO: 此处，使用map函数，不做任何处理，仅仅为了使用累加器
        MapOperator<String, String> ds = dataset.map(new RichMapFunction<String, String>() {
            //TODO:Step1:定义累加器
            private LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO:Step2:注册累加器
                getRuntimeContext().addAccumulator("counter", counter);
            }

            @Override
            public String map(String value) throws Exception {
                // TODO： step3. 使用累加器，进行计数操作
                counter.add(1L);
                return value;
            }
        });
        // 4. 数据终端-sink
        ds.writeAsText("datas/output/longCounter");

        // 5. 触发执行-execute
        JobExecutionResult jobResult = env.execute(_09BatchAccumulatorDemo.class.getSimpleName());

        // TODO: step4. 获取累加器的值
        Object counterValue = jobResult.getAccumulatorResult("counter");
        System.out.println("Counter : " + counterValue);
    }

}
