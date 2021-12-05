package com.itcast.spark.shared

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/17 0017 11:46.
 */
object _05SparkSharedVariableTest {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    //从local文件系统读取数据,封装到RDD中
    val inputRDD: RDD[String] = sc.textFile("datas/filter/datas.input",minPartitions = 2)
    //TODO:字典数据:只要有这些单词就过滤:特殊字符存储列表List中
    val list: List[String] = List(",", ".", "!", "#", "$", "%")
    //TODO:将字典数据进行广播
    val broadcastList: Broadcast[List[String]] = sc.broadcast(list)

    //TODO:定义计数器
    val accumulator: LongAccumulator = sc.longAccumulator("number_accu")

    //2. 调用RDD函数进行转换处理
    val resultRDD: RDD[(String, Int)] = inputRDD
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))
      //TODO:过滤非单词字符
      .filter {
        word =>
          val wordList: List[String] = broadcastList.value
          val flag: Boolean = wordList.contains(word)
          if (flag) {
            accumulator.add(1L)
          }
          //返回
          !flag
      }
      .map((_, 1))
      .reduceByKey(_ + _)

    //4. 将最终处理结果RDD保存到HDFS或者打印在控制台

    resultRDD.foreach(println)

    //累加器的值,必须使用RDD Action函数进行触发
    println("Accumulator = " + accumulator.value)

    //关闭资源
    Thread.sleep(100000)
    sc.stop()
  }

}
