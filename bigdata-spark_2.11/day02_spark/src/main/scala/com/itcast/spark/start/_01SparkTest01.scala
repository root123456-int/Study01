package com.itcast.spark.start

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/15 0015 19:47.
 */
object _01SparkTest01 {
  def main(args: Array[String]): Unit = {
    //1. 创建SparkConf对象,设置属性和对象
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[2]")

    //2. 传递sparkConf对象,构建SparkContext实例对象
    val sc = new SparkContext(sparkConf)
    println(sc)

    Thread.sleep(100000)

    //当应用运行结束,关闭资源
    sc.stop()
  }
}
