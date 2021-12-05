package com.itcast.test_day05.start

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用SparkSQL进行词频统计WordCount：SQL
 */
object _02SparkSQLWordCount {

  def main(args: Array[String]): Unit = {

    // 使用建造设设计模式，创建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    // TODO: 使用SparkSession加载数据
    val inputRDD: RDD[String] = spark.sparkContext.textFile("datas/wordcount/input.data")
    val inputDF: DataFrame = inputRDD
      .filter(line => null != line && line.trim.split("\\s+").length > 0)
      .flatMap(line =>
        line.trim.split("\\s+")
      ).toDF("word")
    /*
      table: words , column: value
          SQL: SELECT value, COUNT(1) AS count  FROM words GROUP BY value
     */
    // step 1. 将Dataset或DataFrame注册为临时视图
    inputDF.createOrReplaceTempView("tmp_view")
    // step 2. 编写SQL并执行
    val result: DataFrame = spark.sql(
      """
        |select
        | word,count(1) total
        |from tmp_view
        |where char_length(word) > 0
        |group by word
        |order by total desc
        |""".stripMargin
    )
    result.show()
    // 应用结束，关闭资源
    spark.stop()
  }

}
