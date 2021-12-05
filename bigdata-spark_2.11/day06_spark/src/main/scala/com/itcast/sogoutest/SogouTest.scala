package com.itcast.sogoutest

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 用户查询日志(SogouQ)分析，数据来源Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。
 * 1. 搜索关键词统计，使用HanLP中文分词
 * 2. 用户搜索次数统计
 * 3. 搜索时间段统计
 * 数据格式：
 * 访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
 * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
 * */
/**
 * @author by FLX
 * @date 2021/6/19 0019 22:31.
 */
object SogouTest {

  def main(args: Array[String]): Unit = {
    //1. 构建SparkSession实例对象
    val spark: SparkSession = getSparkSession(this.getClass)
    //2. 加载数据,封装到RDD
    val inputRDD: RDD[String] = spark.sparkContext.textFile("datas/sogou/SogouQ.reduced")
    //3.ETL转换,将RDD数据转换以后,封装为DataFrame/Dataset
    val sogouDF: DataFrame = process(inputRDD, spark)
    //4.指标分析
    //搜索关键词统计，使用HanLP中文分词
    //reportHanLP(sogouDF)
    //需求二、用户搜索次数统计
    reportCount(sogouDF)
    //需求三、搜索时间段统计, 按照每个小时统计用户搜索次数
    //reportTime(sogouDF)
  }

  //创建SparkSession实例对象,运行本地模式,基本参数设置
  def getSparkSession(clazz: Class[_]): SparkSession = {
    SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "3")
      //设置序列化Kryo
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }

  //将RDD封装到DataFrame/Dataset中
  def process(inputRDD: RDD[String], spark: SparkSession): DataFrame = {
      //导入隐式转换
      import spark.implicits._

    val sogouDF: DataFrame = inputRDD
      .filter(line => null != line && line.trim.split("\\s+").length == 6)
      .mapPartitions { iter =>
        iter.map { line =>
          val Array(queryTime,
          userId,
          queryWords,
          resultRank,
          clickRank,
          clickUrl) = line.trim.split("\\s+")

          SogouRecord(queryTime, userId, queryWords.replaceAll("\\[", "").replace("]", ""),
            resultRank.toInt, clickRank.toInt, clickUrl
          )
        }
      }.toDF()
      //.toDF("queryTime", "userId", "queryWords", "resultRank", "clickRank", "clickUrl")
    //sogouDF.printSchema()
    //sogouDF.show(10,truncate = false)
    sogouDF
  }

  /**
   * 对DataFrame数据按照业务需求进行指标统计分析
   *      需求一、搜索关键词统计，使用HanLP中文分词
   *      需求二、用户搜索次数统计
   *      需求三、搜索时间段统计, 按照每个小时统计用户搜索次数
   */
  def reportHanLP(sogouDF: DataFrame): Unit = {
    //TODO:多个业务分析,缓存数据
    sogouDF.persist(StorageLevel.MEMORY_AND_DISK)
    //需求一、搜索关键词统计，使用HanLP中文分词
    reportSearchKeyWordsDSL(sogouDF)
    reportSearchKeyWordsSQL(sogouDF)
  }
  //需求一、搜索关键词统计，使用HanLP中文分词
  def reportSearchKeyWordsDSL(sogouDF: DataFrame): Unit = {
    val spark: SparkSession = getSparkSession(this.getClass)
    import spark.implicits._
    //自定义UDF函数,进行中文分词
    import org.apache.spark.sql.functions._
    val to_hanlp = udf(
      (queryWords:String) => {
        val terms: util.List[Term] = HanLP.segment(queryWords.trim)
        import scala.collection.JavaConverters._
        terms.asScala.map(_.word).toArray
      }
    )
    val resultDS: Dataset[Row] = sogouDF
      //将搜索关键词分割为单词
      .select(
        explode(to_hanlp($"queryWords")).as("query_word")
      )
      .groupBy($"query_word").count()
      .orderBy($"count".desc)
      .limit(10)
    resultDS.printSchema()
    resultDS.show(10,truncate = false)
    println("================================")
  }
  /**
   * 业务分析指标一：搜索关键词统计，使用HanLP中文分词，基于SQL实现
   */
  def reportSearchKeyWordsSQL(sogouDF: DataFrame): Unit = {
    val spark: SparkSession = getSparkSession(this.getClass)

    spark.udf.register(
      "to_hanlp_sql",
      (queryWords:String) => {
        val terms: util.List[Term] = HanLP.segment(queryWords.trim)
        import scala.collection.JavaConverters._
        terms.asScala.map(_.word).toArray
      }
    )
    sogouDF.createOrReplaceTempView("tmp_view")

    val resultDF: DataFrame = spark.sql(
      """
        |WITH tmp AS (
        |select explode(to_hanlp_sql(queryWords)) as query_word from tmp_view
        |)
        |select query_word,count(1) as total from tmp group by query_word order by total desc limit 10
        |""".stripMargin
    )
    resultDF.printSchema()
    resultDF.show(10,truncate = false)
  }

  //需求二、用户搜索词次数统计
      def reportCount(sogouDF: DataFrame): Unit = {
        //reportCountDSL(sogouDF)
        reportCountSQL(sogouDF)
      }
      def reportCountDSL(sogouDF: DataFrame): Unit = {
        val spark: SparkSession = getSparkSession(this.getClass)
        import spark.implicits._
        import org.apache.spark.sql.functions._
        val resultDF: DataFrame = sogouDF
          .select(
            $"userid",
            $"queryWords"
          )
          .groupBy($"queryWords")
          .agg(
            count($"userID").as("total")
          )
          .orderBy($"total".desc)

        resultDF.printSchema()
        resultDF.show(50,truncate = false)
      }
  def reportCountSQL(sogouDF: DataFrame): Unit = {
    val spark: SparkSession = getSparkSession(this.getClass)
    sogouDF.createOrReplaceTempView("tmp_view")
    val resultDF: DataFrame = spark.sql(
      """
        |select
        |queryWords,count(userId) total
        |from tmp_view
        |group by queryWords
        |order by total desc
        |limit 50
        |""".stripMargin
    )
    resultDF.printSchema()
    resultDF.show(50,truncate = false)
  }

  //需求三、搜索时间段统计, 按照每个小时统计用户搜索次数
  def reportTime(sogouDF: DataFrame): Unit = {
    reportTimeDSL(sogouDF)
    reportTimeSQL(sogouDF)

  }
  def reportTimeDSL(sogouDF: DataFrame): Unit = {
    val spark: SparkSession = getSparkSession(this.getClass)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val resultDF: DataFrame = sogouDF
      .select(
        substring($"queryTime", 0, 2).as("hour"),
        $"userID"
      )
      .groupBy($"hour")
      .agg(
        count($"userID").as("total")
      )
      .orderBy($"total".desc)

    resultDF.printSchema()
    resultDF.show(24,truncate = false)
  }
  def reportTimeSQL(sogouDF: DataFrame): Unit = {
    val spark: SparkSession = getSparkSession(this.getClass)

    sogouDF.createOrReplaceTempView("tmp_view")

    val resultDF: DataFrame = spark.sql(
      """
        |select
        | subString(queryTime,0,2) hour,count(userId) total
        |from tmp_view
        |group by hour
        |order by total desc
        |""".stripMargin
    )
    resultDF.printSchema()
    resultDF.show(24,truncate = false)
  }
}
