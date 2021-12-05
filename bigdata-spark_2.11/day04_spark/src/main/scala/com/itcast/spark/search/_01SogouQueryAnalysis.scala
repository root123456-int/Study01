package com.itcast.spark.search

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author by FLX
 * @date 2021/6/16 0016 21:11.
 */
object _01SogouQueryAnalysis {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }
    //1. 从本地文件系统读取搜素日志数据
    //val rawLogsRDD: RDD[String] = sc.textFile("datas/sogou/SogouQ.sample", minPartitions = 2)
    val rawLogsRDD: RDD[String] = sc.textFile("datas/sogou/SogouQ.reduced", minPartitions = 2)
    //println(s"first:\n ${rawLogsRDD.first()}")
    //println(s"Count:\n ${rawLogsRDD.count()}")

    //2. 解析数据(先过滤不合格的数据),封装样例类SogouRecord对象
    val sogouLogsRDD: RDD[SogouRecord] = rawLogsRDD
      //过滤数据
      .filter(log => null != log && log.trim.split("\\s+").length == 6)
      //解析日志,封装实例对象
      .mapPartitions(iter => {
        iter.map(
          log => {
            //按照分隔符划分数据
            val splitData: Array[String] = log.trim.split("\\s+")
            //构建实例对象
            SogouRecord(
              splitData(0),
              splitData(1),
              splitData(2).replaceAll("\\[", "").replace("]", ""),
              splitData(3).toInt,
              splitData(4).toInt,
              splitData(5)
            )
          })
      })
    //println(s"first:\n ${rawLogsRDD.first()}")
    //println(s"count:\n ${rawLogsRDD.count()}")

    //TODO: 艺具需求对数据进行分析
    /*
    TODO:需求1:需要关键词统计,使用HanLP中文分词
    Step1:获取每条日志数据中[查询词'queryWords']字段数据
    Step2:使用HanLP对查询词进行中文分词
    Step3:按照分词中单词进行词频统计,类似WordCount
         */
    //TODO:获取前Top10
    val top10KeyWordsCountRDD: Array[(String, Int)] = sogouLogsRDD
      //提取查询词字段的值
      .flatMap {
        record =>
          val query: String = record.queryWords
          //使用HanLP分词
          val terms: util.List[Term] = HanLP.segment(query.trim)
          //转换为Scala中集合列表,对每个分词进行处理
          import scala.collection.JavaConverters._
          val words: mutable.Buffer[String] = terms.asScala.map(term => term.word.trim)
          //返回分割单词
          words.toList
      }
      //转换每个分词为二元组,表示分组出现一次
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(10)

    top10KeyWordsCountRDD.foreach(println)

    println("=======================================")
    /*
    需求二、用户搜索次数统计
    TODO： 统计每个用户对每个搜索词的点击次数，二维分组：先对用户分组，再对搜索词分组
    select user_id,query_words,count(*) as total from  records group by user_id,query_words
     */
    val preUserQurryCountRDD: RDD[((String, String), Int)] = sogouLogsRDD
      //提取字段值
      .map(record => {
        val userID: String = record.userId
        val queryWords: String = record.queryWords
        //封装二元组,表示此用户对此检索词进行了1次点击
        ((userID, queryWords), 1)
      })
      //按照Key(先UserId,再queryQords)分组,进行聚合统计
      .reduceByKey(_ + _)

    preUserQurryCountRDD.take(50).foreach(println)

    //获取次数即可
    val countRDD: RDD[Int] = preUserQurryCountRDD.map(_._2)

    println(s"Max : ${countRDD.max()}")
    println(s"Min : ${countRDD.min()}")
    println(s"Avg : ${countRDD.mean()}")


    /*
    需求三、搜索时间段统计, 按照每个小时统计用户搜索次数
				00:00:00  -> 00  提取出小时
     */
    val hourCountRDD: RDD[(Int, Int)] = sogouLogsRDD
      //提取时间字段值
      .map { record =>
        val queryTime: String = record.queryTime
        //获取小时
        val hour: String = queryTime.substring(0, 2)
        //返回二元组
        (hour.toInt, 1)
      }
      //按照小时分组,进行聚合统计
      .foldByKey(0)(_ + _)

    //按照次数进行降序排序
    hourCountRDD
      .top(24)(Ordering.by(_._2))
      .foreach(println)
    sc.stop()
  }
}
