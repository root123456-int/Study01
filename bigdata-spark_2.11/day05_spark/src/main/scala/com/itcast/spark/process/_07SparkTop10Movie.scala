package com.itcast.spark.process

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession, functions}

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
      .master("local[3]")
      //设置Shuffle时分区数目
      .config("spark.sql.shuffle.partitions","3")
      .getOrCreate()

    import spark.implicits._


    // TODO: step1. 读取电影评分数据，从本地文件系统读取，封装数据至RDD中
    val ratingRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-1m/ratings.dat", minPartitions = 3)

    // TODO: step2. 将RDD数据转换为DataFrame，使用toDF指定列名称转换，此时要求RDD数据类型为元组
    /*
      a. 解析每条数据
      b. 构建元组对象
      c. 调用toDF函数，指定列名称
     */
    val ratingDF: DataFrame = ratingRDD
      .filter(line => null != line && line.trim.split("::").length == 4)
      .mapPartitions { iter =>
        iter.map { line =>
          val arr: Array[String] = line.trim.split("::")
          (arr(0), arr(1), arr(2).toDouble, arr(3).toLong)
        }
      }
      .toDF("userid", "movieid", "rating", "timestamp")

    //ratingDF.printSchema()
    //ratingDF.show(10,truncate = false)

    // TODO: step3. 基于SQL方式分析
    /*
      a. 注册为临时视图
      b. 编写SQL，执行分析
     */
    // a. 将DataFrame注册为临时视图
    /*
    ratingDF.createOrReplaceTempView("tmp_view")

    // b. 编写SQL，执行分析
    val resultDF: DataFrame = spark.sql(
      """
        |SELECT
        |movieid,count(*) AS countnum,ROUND(avg(rating),2) AS avgscore
        |FROM tmp_view
        |GROUP BY movieid
        |HAVING countnum > 2000
        |ORDER BY avgscore DESC,countnum DESC
        |LIMIT 10
        |""".stripMargin)

     */

    //resultDF.printSchema()
    //resultDF.show(10,truncate = false)

    // TODO: step3. 使用DSL方式分析数据
    /*
      获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
     */

    val result: Dataset[Row] = ratingDF
      //按照电影ID分组
      .groupBy($"movieid")
      //进行聚合,评分平均值和评分次数
      .agg(
        functions.round(avg($"rating"), 2).as("avgsource"),
        count($"userid").as("countnum")
      )
      //过滤评分次数>2000
      .filter($"countnum" > 2000)
      //按照评分平均值降序排序
      .orderBy($"avgsource".desc)
      //获取前10条
      .limit(10)

    result.printSchema()
    result.show(10,truncate = false)


    // TODO: step 4. 将分析结果数据保存到外部存储系统中，比如保存到MySQL数据库表中或者CSV文件中

    // 4.1 保存结果数据至MySQL表中


    // 4.2 保存结果数据至CSv文件中


    // step5. 应用结束，关闭资源
    Thread.sleep(100000)
    spark.stop()

  }

}
