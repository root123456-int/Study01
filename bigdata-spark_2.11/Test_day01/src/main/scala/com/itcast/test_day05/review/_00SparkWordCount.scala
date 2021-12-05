package com.itcast.test_day05.review

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用Spark实现词频统计WordCount程序
 */
object _00SparkWordCount {

  def main(args: Array[String]): Unit = {
    // TODO: 创建SparkContext实例对象，首先构建SparkConf实例，设置应用基本信息
    /*
    val sc: SparkContext = {
      val sparkCconf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkCconf)
    }

     */
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      // TODO: 设置使用Kryo 序列化方式
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // TODO: 注册序列化的数据类型
      //.registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
      .getOrCreate()
    import spark.implicits._


    // TODO: 第一步、从HDFS读取文件数据，sc.textFile方法，将数据封装到RDD中
    val inputRDD: RDD[String] = spark.sparkContext.textFile("datas/wordcount/input.data")
    inputRDD.take(10).foreach(println)
    println("=======================")

    // TODO: 第二步、调用RDD中高阶函数，进行处理转换处理，函数：flapMap、map和reduceByKey
    /*
      过滤掉空数据
      按照分隔符分割单词
      转换单词为二元组，表示每个单词出现一次
      按照单词分组，对组内执进行聚合reduce操作，求和
     */
    val inputRDD01: RDD[String] = inputRDD
      .filter(line => null != line && line.trim.split("\\s+").length > 0)
      .flatMap(line =>
        line.trim.split("\\s+")
      )
    inputRDD01.foreach(println)

    val inputDF: DataFrame = inputRDD
      .filter(line => null != line && line.trim.split("\\s+").length > 0)
      .flatMap(line =>
        line.trim.split("\\s+")
      ).toDF("word")

    inputDF.printSchema()
    inputDF.show()



/*
    val resultDS: Dataset[Row] = inputDF
      .select(
        "word"
      )
      .groupBy($"word").count()
      .orderBy($"count".desc)

    // TODO: 第三步、将最终处理结果RDD保存到HDFS或打印控制台
    resultDS.printSchema()
    resultDS.show()

 */

    // 应用结束，关闭资源

    spark.stop()
  }
}
