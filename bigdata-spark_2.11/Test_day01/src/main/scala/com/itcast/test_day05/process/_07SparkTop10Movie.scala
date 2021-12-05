package com.itcast.test_day05.process

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 需求：对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
 * 第一步、读取电影评分数据，从本地文件系统读取
 * 第二步、转换数据，指定Schema信息，封装到DataFrame
 * 第三步、基于SQL方式分析
 * 第四步、基于DSL方式分析
 */
object _07SparkTop10Movie {

  def main(args: Array[String]): Unit = {
    // step0. 构建SparkSession实例对象
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("header", "true")
      .getOrCreate()
    import spark.implicits._

    // TODO: step1. 读取电影评分数据，从本地文件系统读取，封装数据至RDD中

    val inputRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-1m/ratings.dat")

    // TODO: step2. 将RDD数据转换为DataFrame，使用toDF指定列名称转换，此时要求RDD数据类型为元组
    /*
      a. 解析每条数据
      b. 构建元组对象
      c. 调用toDF函数，指定列名称
     */
    val inputDF: DataFrame = inputRDD
      .filter(line => null != line && line.trim.split("::").length == 4)
      .map(line => {
        val Array(userId, movieId, rating, timestamp) = line.trim.split("::")
        (userId, movieId, rating.toDouble, timestamp.toLong)
      }).toDF("userId", "movieId", "rating", "timestamp")


    // TODO: step3. 基于SQL方式分析
    /*
      a. 注册为临时视图
      b. 编写SQL，执行分析
     */
    // a. 将DataFrame注册为临时视图
    inputDF.createOrReplaceTempView("tmp_view")
    // b. 编写SQL，执行分析

    val resultDF: DataFrame = spark.sql(
      """
        |select
        |movieId,round(avg(rating),2) avgscore,count(userID) num
        |from
        |tmp_view
        |group by movieId
        |having num > 2000
        |order by avgscore desc,num desc
        |limit 10
        |""".stripMargin
    )

    //resultDF.show()
    // TODO: step3. 使用DSL方式分析数据
    /*
      获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
     */


    // TODO: step 4. 将分析结果数据保存到外部存储系统中，比如保存到MySQL数据库表中或者CSV文件中

    // 4.1 保存结果数据至MySQL表中
/*
    val props: Properties = new Properties()
    props.put("user", "root")
    props.put("password", "123456")
    props.put("driver", "com.mysql.cj.jdbc.Driver")

    resultDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(
        "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
        "db_test.tb_top10_movie",
        props
      )


 */

    // 4.2 保存结果数据至CSv文件中
resultDF
  .coalesce(1)
  .write
  .mode(SaveMode.Overwrite)
  .option("header","true")
  .csv("datas/top10_movies")

    // step5. 应用结束，关闭资源
    spark.stop()
  }
}
