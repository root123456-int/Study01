package cn.itcast.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author by FLX
 * @date 2021/7/16 0016 23:15.
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        //TODO: Flink框架提供参数解析工具类，方便解析参数数据
        ParameterTool tool = ParameterTool.fromArgs(args);

        if (tool.getNumberOfParameters() != 2) {
            System.out.println("Usage: WordCount --host node1.itcast.cn --port 9999 .................");
            System.exit(1);
        }
        final String host = tool.get("host");
        final int port = Integer.parseInt(tool.get("port"));

        //1. 准备环境 - env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. 准备数据 - source
        DataStreamSource<String> inputDataSet = env.socketTextStream(host, port);
        //3. 处理数据 - transformation
        /*
			step1. 将每行数据按照分割符分割为单词
				spark spark flink  ->  spark, spark, flink
			step2. 将每个单词转换为二元组，表示每个单词出现一次
				spark, spark, flink ->  (spark, 1),  (spark, 1), (flink, 1)
			step3. 按照单词分组，将同组中次数进行累加sum
				(spark, 1),  (spark, 1), (flink, 1) -> (spark, 1 + 1 = 2) , (flink, 1 = 1)
		 */
        // step1. 将每行数据按照分割符分割为单词

        SingleOutputStreamOperator<String> wordDataStream = inputDataSet.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.trim().split("\\s+")) {
                    out.collect(word);
                }
            }
        });

        // step2. 将每个单词转换为二元组，表示每个单词出现一次
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDataStream = wordDataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        // step3. 按照单词分组，将同组中次数进行累加sum
		/*
			(spark, 1),  (spark, 1), (flink, 1)
						| keyBy(0) -> 二元组第一个元素，下标索引从0开始
			spark ->  [(spark, 1), (spark, 1)]	,   flink -> [(flink, 1)]
						| sum(1)  ->  对组内数据按照某个值，进行sum求和
			spark ->  (1 + 1 = 2) ,  flink -> (1 = 1)

		 */
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = tupleDataStream.keyBy(0).sum(1);

        //4. 输出结果 - sink
        resultDataStream.print();
        //5. 触发执行 - execute
        env.execute(WordCount.class.getSimpleName());
    }
}
