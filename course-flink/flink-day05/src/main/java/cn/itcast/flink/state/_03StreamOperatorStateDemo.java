package cn.itcast.flink.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Flink State 中OperatorState，自定义数据源Kafka消费数据，保存消费偏移量数据并进行Checkpoint
 */
public class _03StreamOperatorStateDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3) ;

		//TODO:设置检查点Checkpoint相关属性，保存状态
		env.enableCheckpointing(1000) ; //每隔1s执行一次Checkpoint
		env.setStateBackend(new FsStateBackend("file:///D:/ckpt/")) ; //状态数据保存本地文件系统
		env.getCheckpointConfig().enableExternalizedCheckpoints(
			CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
		);//当应用取消时，Checkpoint数据保存，不删除
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		//固定延迟重启策略:程序出现异常的时候，重启2次，每次延迟3秒钟重启，超过2次，程序退出
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.addSource(new KafkaSource());

		// 3. 数据转换-transformation
		// 4. 数据终端-sink
		inputDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(_03StreamOperatorStateDemo.class.getSimpleName()) ;
	}

	/**
	 * 自定义数据源，模拟从Kafka消费数据，管理消费偏移量，进行状态存储
	 */
	private static class KafkaSource extends RichParallelSourceFunction<String>
						implements CheckpointedFunction {
		// TODO: step1. 定义存储偏移量状态
		private transient ListState<Long> offsetState = null ;

		// 标识程序是否运行
		private boolean isRunning = true ;

		// 定义偏移量
		private Long offset = 0L ;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (isRunning){
				// 模拟从Kafka消费数据
				int partitionId = getRuntimeContext().getIndexOfThisSubtask();
				// ..... 省略从Kafka 分区消费数据代码
				offset += 1 ;

				// 输出数据
				ctx.collect("p-" + partitionId + ": " + offset);

				// TODO: step 3. 更新偏移量到状态中
				offsetState.update(Arrays.asList(offset));

				// 每隔1秒消费数据
				TimeUnit.SECONDS.sleep(1);

				// 当偏移量被5整除时，抛出异常
				if(offset % 5 == 0){
					throw new RuntimeException("程序处理异常啦啦啦啦.................") ;
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false ;
		}

		// ==================================================================

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			// 状态描述符
			ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("offsetState", Long.class);
			// TODO: step2. 状态初始化
			offsetState = context.getOperatorStateStore().getListState(descriptor);

			// TODO: 是否从检查点恢复
			if(context.isRestored()){
				// 更新状态值
				offset = offsetState.get().iterator().next();
			}
		}

		// 保存状态到检查点
		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			// 保存数据
			offsetState.clear();
			// 更新状态
			offsetState.update(Arrays.asList(offset));
		}

	}
}
