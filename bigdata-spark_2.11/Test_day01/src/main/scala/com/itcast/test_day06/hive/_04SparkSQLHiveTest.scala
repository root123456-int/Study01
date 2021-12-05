package com.itcast.test_day06.hive

import org.apache.spark.sql.SparkSession

/**
 * SparkSQL集成Hive，读取Hive表的数据进行分析
 */
object _04SparkSQLHiveTest {
	
	def main(args: Array[String]): Unit = {
		
		// TODO: 集成Hive，创建SparkSession实例对象时，进行设置HiveMetaStore服务地址
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[2]")
			.getOrCreate()
		import spark.implicits._
		
		// 方式一、DSL 分析数据
		
		
		println("==================================================")
		// 方式二、编写SQL方式
		
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
