package com.itcast.test_day06.source

import org.apache.spark.sql.SparkSession

/**
 * SparkSQL 外部数据源，加载parquet列式数据，JSON格式和text文本数据
 */
object _02SparkSQLSourceTest {
	
	
	def main(args: Array[String]): Unit = {
		// 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
			.master("local[4]")
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.config("spark.sql.shuffle.partitions", "4")
			.getOrCreate()
		import spark.implicits._
		
		// TODO 1. parquet列式存储数据
		/*
			format方式加载
			parquet方式加载
			load方式加载，在SparkSQL中，当加载读取文件数据时，如果不指定格式，默认是parquet格式数据
		 */
 	
		
		/* =========================================================================== */
		// TODO: 2. 文本数据加载，text -> DataFrame   textFile -> Dataset
		
		
		
		/* =========================================================================== */
		// TODO: 3. 读取JSON格式数据，自动解析，生成Schema信息
	
		
		/* =========================================================================== */
		// TODO: 实际开发中，针对JSON格式文本数据，直接使用text/textFile读取，然后解析提取其中字段信息
		/*
			{"name":"Andy", "salary":30}   - value: String
						| 解析JSON格式，提取字段
				name: String, -> Andy
				salary : Int, -> 30
		 */
		
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
