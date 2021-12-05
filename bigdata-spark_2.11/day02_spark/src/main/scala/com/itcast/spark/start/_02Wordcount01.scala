package com.itcast.spark.start

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/15 0015 19:58.
 */
object _02Wordcount01 {
  def main(args: Array[String]): Unit = {
    //创建SparkContext实例对象,首先构建SparkConf实例,设置应用基本信息
    val sc: SparkContext = {
      //1. 构建SaprkConf对象,设置应用名称和master
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getName)
        .setMaster("local[2]")
      //2. 创建SparkContext实例,传递sparkConf
      new SparkContext(sparkConf)
    }
    // 1. 从HDFS读取数据,sc.txtFile方法,将数据封装到RDD中
    val inputRDD: RDD[String] = sc.textFile("hdfs://192.168.88.100:8020/datas/wordcount.data")
    //val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data")
    //2.调用RDD中高阶函数,进行处理转换,函数:flagMap,map,reduceByKey
    val resultRDD: RDD[(String, Int)] = inputRDD
      //按照分隔符分割单词
      .flatMap(_.split("\\s+"))
      //转换为二元组
      .map((_, 1))
      //按照单词分组,对组内女性reduce操作,求和
      .reduceByKey((tmp,item) => tmp + item)

    //resultRDD.saveAsTextFile("/spark/datas/output-wordcount")

    resultRDD.foreach(println)

    Thread.sleep(10000)

    sc.stop()
  }
}
