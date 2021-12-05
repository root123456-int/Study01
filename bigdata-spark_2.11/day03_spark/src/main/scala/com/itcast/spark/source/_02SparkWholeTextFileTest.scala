package com.itcast.spark.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/15 0015 22:52.
 */
object _02SparkWholeTextFileTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    /*
		  def wholeTextFiles(
		      path: String,
		      minPartitions: Int = defaultMinPartitions
		  ): RDD[(String, String)]

		  将每个文件数据读取到RDD的1条记录中,返回类型为 Key Value对
		  Key: 每个小文件名称路径
		  Value：每个小文件的内容
		 */

    val inputRDD: RDD[(String, String)] = sc.wholeTextFiles(
      "datas/ratings100",
      minPartitions = 2
    )

    println("Partitions =" + inputRDD.getNumPartitions)

    println(s"Count = ${inputRDD.count()}")

    println("first:\n" + inputRDD.first())
    sc.stop()
  }

}
