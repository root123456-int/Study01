package com.itcast.spark.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/17 0017 11:03.
 */
object _04SparkWriteMySQL {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }
    //1.从loacl文件系统,读取数据,封装到RDD中
    val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data")

    //2.调用RDD函数进行转换处理
    val resultRDD: RDD[(String, Int)] = inputRDD
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)

    //TODO:对结果数据esultRDD保存到MySQL中
    /*
    1. 对结果数据降低分区数目
    2. 针对每个分区数据进行操作
      每个分区数据插入数据库时,创建一个连接Contection
     */

    resultRDD
      //降低分区数目,结果中数目变少了
      .coalesce(1)
      //将数据保存到MySQL表中,针对每个分区操作
      .foreachPartition(iter => savaToMySQL(iter))

    sc.stop()
  }

  /*
  定义一个方法,将RDD中分区数据保存到MySQL表
   */
  def savaToMySQL(iter: Iterator[(String, Int)]) = {
    //1.加载驱动类
    Class.forName("com.mysql.cj.jdbc.Driver")

    //声明变量
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      //2.创建链接
      conn = DriverManager.getConnection(
        "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
        "root",
        "123456"
      )
      pstmt = conn.prepareStatement("insert into db_test.tb_wordcount (word, count) VALUES(?, ?)")

      //3.插入数据
      iter.foreach {
        case (word, count) =>
          //设置占位符的值
          pstmt.setString(1, word)
          pstmt.setString(2, count.toString)
          //单条插入
          //pstmt.execute()

          //TODO:加入批次
          pstmt.addBatch()
      }
      //TODO:批量插入
      pstmt.executeBatch()
    } catch {
      case e: Exception => e.printStackTrace()
    }finally {
      //4. 关闭连接
      if (null != pstmt) pstmt.close()
      if (null != conn) conn.close()
    }
  }
}
