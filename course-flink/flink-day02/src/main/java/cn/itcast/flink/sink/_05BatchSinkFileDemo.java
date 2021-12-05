package cn.itcast.flink.sink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;

import java.util.List;

/**
 * DataSet API 批处理中数据终端：基于文件Sink
	 * 1.ds.print 直接输出到控制台
	 * 2.ds.printToErr() 直接输出到控制台,用红色
	 * 3.ds.collect 将分布式数据收集为本地集合
	 * 4.ds.setParallelism(1).writeAsText("本地/HDFS的path",WriteMode.OVERWRITE)
 *
 * 注意: 在输出到path的时候，可以在前面设置并行度，如果
	 * 并行度>1，则path为目录
	 * 并行度=1，则path为文件名
 */
public class _05BatchSinkFileDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		// 2. 数据源-source

		DataSource<String> ds01 = env.readTextFile("datas/wordcount.data");

		// 3. 数据终端-sink
		// 方式一、方式二：控制台

		// 方式三：转换为本地集合
		//List<String> list = ds01.collect();
		//System.out.println(list);

		// 方式四：保存为文件

		ds01.writeAsText("datas/output/s1");
		// 方式五：保存为CSV文件 TODO:数据类型必须是二元组
		ds01.map(new RichMapFunction<String, Tuple2<Integer,String>>() {
			//将数据转换为二元组
			@Override
			public Tuple2<Integer, String> map(String line) throws Exception {
				//获取数据的分区ID
				int index = getRuntimeContext().getIndexOfThisSubtask();

				//返回二元组对象
				return Tuple2.of(index,line);
			}
		})
				//保存为CSV文件
		.writeAsCsv("datas/output/s2-csv", FileSystem.WriteMode.OVERWRITE);

		// 5. 触发执行-execute
		//TODO:如果保存数据到外部系统比如文件中,需要出发执行
		env.execute("BatchSinkFileDemo");

	}

}
