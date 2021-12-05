package com.itcast.spark.ckpt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/16 0016 20:26.
 */
object _09SparkCkptTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    //TODO:设置检查点目录,将RDD数据保存到那个目录
    sc.setCheckpointDir("datas/ckpt-rdd")

    val datasRDD: RDD[String] = sc.textFile("datas/wordcount.data")


    //TODO:调用checkpoint函数,将RDD进行备份,需要RDD中Action函数触发
    datasRDD.checkpoint()
    //checkpoint属于懒执行(lazy),需要RDD中Action函数触发
    datasRDD.count()

    //TODO:再次执行count函数,此时从checkpoint读取数据
    println(s"count = ${datasRDD.count()}")

    Thread.sleep(100000)
    sc.stop()

  }

}
