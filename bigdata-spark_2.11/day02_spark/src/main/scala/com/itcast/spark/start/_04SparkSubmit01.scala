package com.itcast.spark.start

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/15 0015 20:19.
 */
object _04SparkSubmit01 {
  def main(args: Array[String]): Unit = {

    //判断是否传递了2个参数,如果不是直接抛出异常
    if (args.length <2 ) {
      println("Usage:SparkSubmit<input><output>.........")
      System.exit(-1)
    }

    val sc = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName("SaprkWordCount")
        //.setMaster("local[2]")
      new SparkContext(sparkConf)
    }


    val inputRDD: RDD[String] = sc.textFile(args(0))

    val resultRDD: RDD[(String, Int)] = inputRDD
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((tem, item) => tem + item)

    //resultRDD.foreach(result => println(result))

    resultRDD.saveAsTextFile(s"${args(1)}-${System.currentTimeMillis()}")
    //Thread.sleep(10000)
    sc.stop()
  }
}