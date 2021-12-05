package com.itcast.test_day06.thrift

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
  * SparkSQL 启动ThriftServer服务，通过JDBC方式访问数据分析查询
  */
object _06SparkThriftJDBCTest {

	def main(args: Array[String]): Unit = {

		// 定义相关实例对象，未进行初始化
		var conn: Connection = null
		var pstmt: PreparedStatement = null
		var rs: ResultSet = null

		try {
			// TODO： a. 加载驱动类
			Class.forName("org.apache.hive.jdbc.HiveDriver")
			// TODO: b. 获取连接Connection
			conn = DriverManager.getConnection(
				"jdbc:hive2://node1.itcast.cn:10000/db_hive",
				"root",
				"123456"
			)
			// TODO: c. 构建查询语句
			val sqlStr: String =
				"""
				  |select e.empno, e.ename, e.sal, d.dname from emp e join dept d on e.deptno = d.deptno
				""".stripMargin
			pstmt = conn.prepareStatement(sqlStr)
			// TODO: d. 执行查询，获取结果
			rs = pstmt.executeQuery()
			// 打印查询结果
			while (rs.next()) {
				println(s"empno = ${rs.getInt(1)}, ename = ${rs.getString(2)}, sal = ${rs.getDouble(3)}, dname = ${rs.getString(4)}")
			}
		} catch {
			case e: Exception => e.printStackTrace()
		} finally {
			if (null != rs) rs.close()
			if (null != pstmt) pstmt.close()
			if (null != conn) conn.close()
		}
	}

}
