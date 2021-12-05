package com.itcast.spark.func.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author by FLX
 * @date 2021/6/16 0016 18:13.
 */
object _04SparkJoinTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }
    //模拟数据集
    val empRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhaoliu"))
    )
    val deptRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "sales"), (1002, "tech"))
    )

    //TODO:等值连接
    val joinRDD: RDD[(Int, (String, String))] = empRDD.join(deptRDD)

    joinRDD.foreach {
      case (deptno, (empname, deptname)) =>
        println(s"deptno = ${deptno},empname = ${empname},deptname = ${deptname}")
    }
    println("==============================")

    val leftRDD: RDD[(Int, (String, Option[String]))] = empRDD.leftOuterJoin(deptRDD)

    leftRDD.foreach {
      case (deptno, (empname, option)) =>
        val deptname: String = option match {
          case Some(name) => name
          case None => "未知"
        }
        println(s"deptno = ${deptno}, empname = ${empname}, deptname = ${deptname}")
    }
  }
}
