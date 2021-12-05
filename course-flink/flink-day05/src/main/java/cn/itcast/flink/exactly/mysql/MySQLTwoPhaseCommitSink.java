package cn.itcast.flink.exactly.mysql;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.codehaus.jackson.node.ObjectNode;
import sun.reflect.generics.tree.VoidDescriptor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 自定义kafka to mysql，继承TwoPhaseCommitSinkFunction,实现两阶段提交。
    功能：保证kafak to mysql 的Exactly-Once
	 CREATE TABLE `t_test` (
		 `id` bigint(20) NOT NULL AUTO_INCREMENT,
		 `value` varchar(255) DEFAULT NULL,
		 `insert_time` datetime DEFAULT NULL,
		 PRIMARY KEY (`id`)
	 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;
 */
public class MySQLTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<String, Connection, Void> {

	private Connection connection = null ;

	public MySQLTwoPhaseCommitSink(){
		super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
	}

	/**
	 * 执行数据入库操作
	 */
	@Override
	protected void invoke(Connection transaction, String value, Context context) throws Exception {
		System.err.println("start invoke.......");
		String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		System.err.println("===>date:" + date + " " + value);

		String sql = "insert into `t_test` (`value`,`insert_time`) values (?,?)";
		PreparedStatement ps = connection.prepareStatement(sql);
		ps.setString(1, value);
		ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));

		//执行insert语句
		ps.execute();

		//手动制造异常
		if(Integer.parseInt(value) == 15) System.out.println(1/0);
	}

	/**
	 * 获取连接，开启手动提交事物（getConnection方法中）
	 */
	@Override
	protected Connection beginTransaction() throws Exception {
		String url = "jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
		Connection connection = DBConnectUtil.getConnection(url, "root", "123456");
		System.err.println("start beginTransaction......."+connection);

		return connection;
	}

	/**
	 * 预提交，这里预提交的逻辑在invoke方法中
	 */
	@Override
	protected void preCommit(Connection transaction) throws Exception {
		System.err.println("start preCommit......."+connection);
	}

	/**
	 * 如果invoke执行正常则提交事物
	 */
	@Override
	protected void commit(Connection transaction) {
		System.err.println("start commit......."+connection);
		DBConnectUtil.commit(connection);
	}

	@Override
	protected void recoverAndCommit(Connection connection) {
		System.err.println("start recoverAndCommit......."+connection);

	}

	@Override
	protected void recoverAndAbort(Connection connection) {
		System.err.println("start abort recoverAndAbort......."+connection);
	}

	/**
	 * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
	 */
	@Override
	protected void abort(Connection transaction) {
		System.err.println("start abort rollback......."+connection);
		DBConnectUtil.rollback(connection);
	}
}
