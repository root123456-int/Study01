package com.itcast.spark.ds

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 采用反射的方式将RDD转换为Dataset，要求RDD中数据类型为CaseClass
 */
object _01SparkDatasetTest {

  def main(args: Array[String]): Unit = {

    // 构建SparkSession实例对象，设置应用名称和master
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._

    // 1. 加载电影评分数据，封装数据结构RDD
    val rawRatingRDD: RDD[String] = spark.sparkContext.textFile("datas/ml-100k/u.data", minPartitions = 3)

    // 2. 将RDD数据类型转化为 MovieRating
    /*
      将原始RDD中每行数据（电影评分数据）封装到CaseClass样例类中
     */
    val ratingRDD: RDD[MovieRating] = rawRatingRDD
      .filter(line => null != line && line.trim.split("\\s+").length == 4)
      .mapPartitions { iter =>
        iter.map { line =>
          val Array(userId, itemId, rating, timestamp) = line.trim.split("\\s+")
          MovieRating(userId, itemId, rating.toDouble, timestamp.toLong)
        }
      }

    // TODO: 3. 将RDD转换为Dataset，可以通过隐式转， 要求RDD数据类型必须是CaseClass
    val resultDS: Dataset[MovieRating] = ratingRDD.toDS()
    //resultDS.printSchema()
    //resultDS.show(10, truncate = false)
    /*
      Dataset 从Spark1.6提出
        Dataset = RDD + Schema
        DataFrame = RDD[Row] + Schema
        Dataset[Row] = DataFrame
     */
    // 从Dataset中获取RDD
    val rdd: RDD[MovieRating] = resultDS.rdd
    val schema: StructType = resultDS.schema
    // 从Dataset中获取DataFrame
    val resultDF: DataFrame = resultDS.toDF()
    // 给DataFrame加上强类型（CaseClass）就是Dataset

    /*
      DataFrame中字段名称与CaseClass中字段名称一致
     */
    val dataset: Dataset[MovieRating] = resultDF.as[MovieRating]

    // 应用结束，关闭资源
    //Thread.sleep(100000)
    spark.stop()
  }

}
