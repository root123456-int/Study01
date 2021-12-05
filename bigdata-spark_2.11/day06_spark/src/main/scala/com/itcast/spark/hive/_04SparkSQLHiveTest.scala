package com.itcast.spark.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SparkSQL集成Hive，读取Hive表的数据进行分析
 */
object _04SparkSQLHiveTest {

  def main(args: Array[String]): Unit = {

    // TODO: 集成Hive，创建SparkSession实例对象时，进行设置HiveMetaStore服务地址
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      //设置Hive MetaStore地址
      .config("hive.metastore.uris", "thrift://node1.itcast.cn:9083")
			//显示指定,集成Hive
			.enableHiveSupport()
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    // 方式一、DSL 分析数据
    val dfHive01: DataFrame = spark.read
      .table("db_hive.emp")
    dfHive01.printSchema()
    dfHive01.show(10, truncate = false)

    println("==================================================")
    // 方式二、编写SQL方式

		val dfHive02: DataFrame = spark
			.sql(
				"select * from db_hive.emp"
			)
		dfHive02.printSchema()
		dfHive02.show(10,truncate = false)


    // 应用结束，关闭资源
    spark.stop()
  }

}
