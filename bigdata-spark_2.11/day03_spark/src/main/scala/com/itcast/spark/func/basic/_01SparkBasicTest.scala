package com.itcast.spark.func.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/16 0016 0:14.
 */
object _01SparkBasicTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this
          .getClass
          .getSimpleName
          .stripSuffix("$"))
        .setMaster("local[2]")

      SparkContext.getOrCreate(sparkConf)
    }
    val inputRDD: RDD[String] = sc.textFile("datas/wordcount/input.data", minPartitions = 2)

    val resultRDD: RDD[(String, Int)] = inputRDD
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)

    resultRDD.foreach(println(_))

    sc.stop()
  }
}
