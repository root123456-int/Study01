package com.itcast.spark.func.agg

import org.apache.spark.rdd.RDD
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/16 0016 17:46.
 */
object ScalaAggTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    val inputRDD: RDD[Int] = sc.parallelize(1 to 10, numSlices = 2)

    //reduce

    val reduceResult: Int = inputRDD.reduce(
      (tem, item) => {
        println(s"tem = ${tem},item = ${item},tem + item = ${tem + item}")
        tem + item
      }
    )
    println("reduceResult:" + reduceResult)

    println("============================================")
    //fold

    val foldResult: Int = inputRDD.fold(0)(
      (tem, item) => {
        println(s"tem = ${tem},item = ${item},tem + item = ${tem + item}")
        tem + item
      }
    )
    println(foldResult)
    println("============================================")
    //aggregate

    val aggResult: Int = inputRDD.aggregate(0)(
      (tem, item) => {
        println(s"tem = ${tem},item = ${item},tem + item = ${tem + item}")
        tem + item
      },

      (tem, item) => {
        println(s"tem = ${tem},item = ${item},tem + item = ${tem + item}")
        tem + item
      }
    )

    println("aggResult " + aggResult)
  }
}
