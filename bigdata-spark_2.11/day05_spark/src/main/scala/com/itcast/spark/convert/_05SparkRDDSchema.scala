package com.itcast.spark.convert

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
    import spark.implicits._
    // 1. 加载电影评分数据，封装数据结构RDD
    val ratingRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-100k/u.data", minPartitions = 3)

    // 2. TODO: step1. RDD中数据类型为Row：RDD[Row]
    val rdd: RDD[Row] = ratingRDD
      .filter(line => null != line && line.trim.split("\\s+").length == 4)
      .mapPartitions { iter =>
        iter.map { line =>
          val Array(userid,movieid,rating,timestamp)= line.trim.split("\\s+")
          //封装样例类
          Row(userid,movieid,rating.toDouble,timestamp.toLong)
        }
      }

    // 3. TODO：step2. 针对Row中数据定义Schema：StructType
    val schema: StructType = StructType(
      Array(
        StructField("userid", StringType, nullable = true),
        StructField("movieid", StringType, nullable = true),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )

    // 4. TODO：step3. 使用SparkSession中方法将定义的Schema应用到RDD[Row]上
    val ratingDF: DataFrame = spark.createDataFrame(rdd, schema)

    ratingDF.printSchema()
    ratingDF.show(10, truncate = false)
    // 应用结束，关闭资源
    spark.stop()
  }

}
