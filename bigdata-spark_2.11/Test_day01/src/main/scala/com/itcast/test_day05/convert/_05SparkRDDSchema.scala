package com.itcast.test_day05.convert

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.CurrentRow.nullable
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

/**
 * 自定义Schema方式转换RDD为DataFrame
 */
object _05SparkRDDSchema {

  def main(args: Array[String]): Unit = {

    // 构建SparkSession实例对象，设置应用名称和master
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()

    // 1. 加载电影评分数据，封装数据结构RDD
    val inputRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-100k/u.data")

    // 2. TODO: step1. RDD中数据类型为Row：RDD[Row]
    val transferRDD: RDD[Row] = inputRDD
      .filter(line => null != line && line.trim.split("\\s+").length == 4)
      .map(line => {
        val arr: Array[String] = line.trim.split("\\s+")
        Row(arr(0), arr(1), arr(2).toDouble, arr(3).toLong)
      })

    // 3. TODO：step2. 针对Row中数据定义Schema：StructType
    val schame: StructType = new StructType(
      Array(
        StructField("userId", StringType, nullable = false),
        StructField("itemId", StringType, nullable = false),
        StructField("rating", DoubleType, nullable = false),
        StructField("timestamp", LongType, nullable = false)
      )
    )

    // 4. TODO：step3. 使用SparkSession中方法将定义的Schema应用到RDD[Row]上
    val inputDF: DataFrame = spark.createDataFrame(transferRDD, schame)
    inputDF.printSchema()
    inputDF.show(10,truncate = false)
    // 应用结束，关闭资源
    spark.stop()
  }

}
