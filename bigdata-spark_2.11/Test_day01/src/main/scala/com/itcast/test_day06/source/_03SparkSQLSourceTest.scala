package com.itcast.test_day06.source

import org.apache.spark.sql.SparkSession

/**
 * SparkSQL外部数据源，加载CSV和MySQL表的数据
 */
object _03SparkSQLSourceTest {
	
	def main(args: Array[String]): Unit = {
		// 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
			.master("local[4]")
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.config("spark.sql.shuffle.partitions", "4")
			.getOrCreate()
		import spark.implicits._
		
		// TODO: 1. CSV 格式数据文本文件数据 -> 依据 CSV文件首行是否是列名称，决定读取数据方式不一样的
		/*
			CSV 格式数据：
				每行数据各个字段使用逗号隔开
				也可以指的是，每行数据各个字段使用 单一 分割符 隔开数据
		 */
		// 方式一：首行是列名称，数据文件u.dat

		
		// 方式二：首行不是列名，需要自定义Schema信息，数据文件u.data
		
		
		/* ============================================================================== */
		// TODO: 2. 读取MySQL表中数据
		/*
			("url", "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
			("driver", "com.mysql.cj.jdbc.Driver")
			("user", "root")
			("password", "123456")
			("dbtable", table)
		 */
		// 第一、简洁版格式
		
		
		// 第二、标准格式写

		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
