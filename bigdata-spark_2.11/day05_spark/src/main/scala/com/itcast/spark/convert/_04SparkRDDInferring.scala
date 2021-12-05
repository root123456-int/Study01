package com.itcast.spark.convert

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 采用反射的方式将RDD转换为DataFrame和Dataset
 */
object _04SparkRDDInferring {

  def main(args: Array[String]): Unit = {

    // 构建SparkSession实例对象，设置应用名称和master
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._

    // 1. 加载电影评分数据，封装数据结构RDD
    val ratingRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-100k/u.data", minPartitions = 3)

    // 2. 将RDD数据类型转化为 MovieRating
    val rdd: RDD[MovieRating] = ratingRDD
      .filter(line => null != line && line.trim.split("\\s+").length == 4)
      .mapPartitions { iter =>
        iter.map { line =>
          val Array(user_id, item_id, rating, timestamp) = line.trim.split("\\s+")
          MovieRating(user_id, item_id, rating.toDouble, timestamp.toLong)
        }
      }

    // 3. 通过隐式转换，直接将CaseClass类型RDD转换为DataFrame
    val dataFrame: DataFrame = rdd.toDF()

    dataFrame.printSchema()
    dataFrame.show(10,truncate = false)
    // 应用结束，关闭资源
    spark.stop()
  }

}
