package com.itcast.spark.point

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Spark 2.x开始，提供了SparkSession类，作为Spark Application程序入口，
 *      用于读取数据和调度Job，底层依然为SparkContext
 */
object _01SparkStartPoint {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 使用建造者设计模式，创建SparkSession实例对象
		val spark: SparkSession = SparkSession
			.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[2]")
			.getOrCreate()
		
		// 2. TODO: 使用SparkSession加载数据
		val inputDS: Dataset[String] = spark.read.textFile("datas/wordcount.data")
		// 3. 显示前5条数据
		println(s"count = ${inputDS.count()}")
		inputDS.show(5,truncate = false)

		
		// 4. 应用结束，关闭资源
		spark.stop()
		
	}
}
