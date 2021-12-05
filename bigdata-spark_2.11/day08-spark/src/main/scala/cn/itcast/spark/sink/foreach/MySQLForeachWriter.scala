package cn.itcast.spark.sink.foreach

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}

/**
 * 创建类继承ForeachWriter，将数据写入到MySQL表中，泛型为：Row，针对DataFrame操作，每条数据类型就是Row
 */
class MySQLForeachWriter extends ForeachWriter[Row] {

  // 定义变量
  var conn: Connection = _
  var pstmt: PreparedStatement = _

  val driver: String = "com.mysql.cj.jdbc.Driver"
  val jdbcUrl: String = "jdbc:mysql://192.168.88.100:3306/db_spark?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true"
  val insertSQL = "REPLACE INTO `tb_word_count` (`id`, `word`, `count`) VALUES (NULL, ?, ?)"

  //创建数据库连接
  override def open(partitionId: Long, epochId: Long): Boolean = {
    //加载驱动类
    Class.forName(driver)
    //获取链接
    conn = DriverManager.getConnection(jdbcUrl, "root", "123456")
    //获取PreparedStatement对象
    pstmt = conn.prepareStatement(insertSQL)

    //返回True,表示连接成功
    true
  }

  //将每条数据写入MySQL数据库中
  override def process(row: Row): Unit = {
    //设置具体值
    pstmt.setString(1,row.getAs[String]("word"))
    pstmt.setInt(2,row.getAs[Long]("count").toInt)

    //执行
    pstmt.executeUpdate()
  }

  //启动流式应用完成时,关闭连接
  override def close(errorOrNull: Throwable): Unit = {
    if (null != pstmt) pstmt.close()
    if (null != conn) conn.close()
  }
}
