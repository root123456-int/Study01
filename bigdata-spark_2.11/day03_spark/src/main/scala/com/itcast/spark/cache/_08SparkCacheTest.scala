package com.itcast.spark.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/16 0016 20:04.
 */
object _08SparkCacheTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }
    val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data", minPartitions = 2)

    //缓存数据:将数据缓存到内存中
    inputRDD.cache()
    //使用Action函数触发
    inputRDD.count()

    //释放缓存
    inputRDD.unpersist()


    //缓存数据,选择缓存级别

    inputRDD.persist(StorageLevel.MEMORY_AND_DISK)

    Thread.sleep(100000)
    sc.stop()
  }

}
