package com.itcast.spark.func.iter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/16 0016 17:15.
 */
object _05SparkPartitionTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    //1. 读取数据
    val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data", minPartitions = 2)

    println(s"原始的分区数目:${inputRDD.getNumPartitions}")

    //TODO:增加RDD数目
    val etlRDD: RDD[String] = inputRDD.repartition(3)
    println(s"增加分区数目后:${etlRDD.getNumPartitions}")

    //2. 处理数据
    val resultRDD: RDD[(String, Int)] = inputRDD
      //数据过滤
      .filter(line => null != line && line.trim.length != 0)
      //对每行数据进行分割
      .flatMap(line => line.trim.split("\\s+"))
      //转换为二元组
      .mapPartitions(iter => iter.map(word => (word, 1)))
      //分组聚合
      .reduceByKey(_ + _)

    //3.输出数据
    resultRDD.foreachPartition(iter => iter.foreach(println(_)))
    println(s"计算结果RDD的分区数目:${resultRDD.getNumPartitions}")

    //降低分区数目
    val outputRDD: RDD[(String, Int)] = resultRDD.coalesce(1)
    println(s"减低分区数目后:${outputRDD.getNumPartitions}")

    //关闭资源
    sc.stop()
  }
}
