package com.itcast.spark.start

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用SparkSQL进行词频统计WordCount：SQL
 */
object _00SparkSQLWordCount {
	
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
			root
			 |-- value: string (nullable = true)
		 */
		//inputDS.printSchema()
		/*
			+--------------------+
			|               value|
			+--------------------+
			|hadoop spark hado...|
			|mapreduce spark  ...|
			|hive spark hadoop...|
			+--------------------+
		 */
		//inputDS.show(5, truncate = false)
		
		/*
			table: words , column: value
					SQL: SELECT value, COUNT(1) AS count  FROM words GROUP BY value
		 */
		// step 1. 将Dataset或DataFrame注册为临时视图
		inputDS.createOrReplaceTempView("tmp_view_line")
		
		// step 2. 编写SQL并执行
		val resultDF: DataFrame = spark.sql(
			"""
			  |WITH tmp AS(
			  |  SELECT explode(split(trim(value), "\\s+")) AS word FROM tmp_view_line
			  |)
			  |SELECT word, COUNT(1) AS total FROM tmp GROUP BY word
			  |""".stripMargin)
		resultDF.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
