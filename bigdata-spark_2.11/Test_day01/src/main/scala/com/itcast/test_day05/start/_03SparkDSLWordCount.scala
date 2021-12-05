package com.itcast.test_day05.start

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用SparkSQL进行词频统计WordCount：DSL
 */
object _03SparkDSLWordCount {
	
	def main(args: Array[String]): Unit = {
		
		// 使用建造设设计模式，创建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[2]")
			.getOrCreate()
		import spark.implicits._
		
		// TODO: 使用SparkSession加载数据
		val inputDS: Dataset[String] = spark.read.textFile("datas/wordcount.data")
		
		
		/*
			table: words , column: value
					SQL: SELECT word, COUNT(1) AS count  FROM words GROUP BY word
		 */
	
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
