package com.itcast.spark.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/15 0015 21:50.
 */

/**
 * Spark 采用并行化的方式构建Scala集合Seq中的数据为RDD
 * - 将Scala集合转换为RDD
 *      sc.parallelize(seq)
 * - 将RDD转换为Scala中集合
 * rdd.collect() -> Array
 * rdd.collectAsMap() - Map,要求RDD数据类型为二元组
 */

object _01SparkParallelizeTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    val lineSeq: Seq[String] = Seq("hadoop scala hive spark scala sql sql", //
    "hadoop scala spark hdfs hive spark", //
      "spark hdfs spark hdfs scala hive spark")

    val inputRDD: RDD[String] = sc.parallelize(lineSeq, numSlices = 2)

    val resultRDD: RDD[(String, Int)] = inputRDD.flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)

    resultRDD.foreach(println(_))

    sc.stop()
  }
}
