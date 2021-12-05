package com.itcast.test_day06.start

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用SparkSQL进行词频统计WordCount：DSL
 */
object _00SparkDSLWordCount {
	
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
			+----------------------------------------+
			|value                                   |
			+----------------------------------------+
			|hadoop spark hadoop spark spark         |
			|mapreduce spark spark hive              |
			|hive spark hadoop mapreduce spark       |
			|spark hive sql sql spark hive hive spark|
			|hdfs hdfs mapreduce mapreduce spark hive|
			+----------------------------------------+
		 */
		//inputDS.show(10, truncate = false)
		
		/*
			table: words , column: value
					SQL: SELECT word, COUNT(1) AS count  FROM words GROUP BY word
		 */
		val resultDS: DataFrame = inputDS
			// hadoop spark hadoop spark spark  -> 分割单词，并且扁平化
			.select(
				explode(split(trim($"value"), "\\s+")).as("word")
			)
			.groupBy("word").count()
		/*
		root
		 |-- word: string (nullable = true)
		 |-- count: long (nullable = false)
		 */
		resultDS.printSchema()
		/*
			+---------+-----+
			|word    |count|
			+---------+-----+
			|sql      |2    |
			|spark    |11   |
			|mapreduce|4    |
			|hdfs     |2    |
			|hadoop   |3    |
			|hive     |6    |
			+---------+-----+
		 */
		resultDS.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
