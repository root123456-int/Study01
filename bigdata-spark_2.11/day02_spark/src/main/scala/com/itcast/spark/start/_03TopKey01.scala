package com.itcast.spark.start

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/15 0015 20:19.
 */
object _03TopKey01 {
  def main(args: Array[String]): Unit = {


    val sc = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getName)
        .setMaster("local[2]")
      new SparkContext(sparkConf)
    }


    val inputRDD: RDD[String] = sc.textFile("/spark/datas/wordcount.data")

    val resultRDD: RDD[(String, Int)] = inputRDD.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((tem, item) => tem + item)

    resultRDD.foreach(result => println(result))

    println("============================================")

    println("=======sortByKey=======")
    resultRDD.map(tuple => tuple.swap)
      .sortByKey(ascending = false)
      .take(3)
      .foreach(tuple => println(tuple))

    println("=======sortBy==========")
    /*
    def sortBy[K](
              f: (T) => K, // 指定排序规则
              ascending: Boolean = true,
              numPartitions: Int = this.partitions.length
          )
          (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
     */
    resultRDD.sortBy(
      tuple => tuple._2, ascending = false
    )
      .take(3)
      .foreach(println(_))

    println("==========Top=========")

    //def top(num: Int)(implicit ord: Ordering[T]): Array[T]

    resultRDD.top(3)(Ordering.by(tuple => tuple._2))
      .foreach(println(_))

    Thread.sleep(10000)
    sc.stop()
  }
}