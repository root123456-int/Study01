package com.itcast.spark.func.iter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/16 0016 1:01.
 */
object _04SparkIterTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
        }

    val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data", minPartitions = 2)

    val resultRDD: RDD[(String, Int)] = inputRDD
      .filter(line => line.trim.length != 0)
      .flatMap(line => line.trim.split("\\s+"))
      //.map(word => word -> 1)
      /*
        def mapPartitions[U: ClassTag](
            f: Iterator[T] => Iterator[U],
            preservesPartitioning: Boolean = false
        ): RDD[U]
       */
      .mapPartitions(iter => iter.map(word => (word, 1)))
      .reduceByKey(_ + _)


    //resultRDD.foreach(item => println(item))
    /*
      def foreachPartition(f: Iterator[T] => Unit): Unit
     */
    resultRDD.foreachPartition(iter => iter.foreach(item => println(item)))

    sc.stop()
  }
}
