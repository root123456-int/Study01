package com.itcast.spark.udf

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SparkSQL中UDF函数定义与使用：分别在SQL和DSL中
 */
object _05SparkUdfTest {
	
	def main(args: Array[String]): Unit = {
		
		// 构建SparkSession实例对象，设置应用名称和master
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			.getOrCreate()
		import spark.implicits._
		
		val empDF: DataFrame = spark.read.json("datas/resources/employees.json")
		/*
			root
			 |-- name: string (nullable = true)
			 |-- salary: long (nullable = true)
		 */
		//empDF.printSchema()
		/*
			+-------+------+
			|name   |salary|
			+-------+------+
			|Michael|3000  |
			|Andy   |4500  |
			|Justin |3500  |
			|Berta  |4000  |
			+-------+------+
		 */
		//empDF.show(10, truncate = false)
		
		/*
			自定义UDF函数功能：将某个列数据，转换为大写
		 */
		// TODO: 在SQL中使用
		spark.udf.register(
			"to_upper01",	//函数名称
			(word:String) => {
				word.trim.toUpperCase
			}
		)

		empDF.createOrReplaceTempView("view_temp_emp")
		//编写SQL并执行
		spark.sql("select name,to_upper01(name) as new from view_temp_emp").show()
		
		println("=====================================================")
		
		// TODO: 在DSL中使用
		import org.apache.spark.sql.functions.udf
		val to_upper02 = udf (
			(word:String) => {
				word.trim.toUpperCase
			}
		)

		empDF
			.select(
				$"name",
				to_upper02($"name").as("new")
			)
			.show()
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
