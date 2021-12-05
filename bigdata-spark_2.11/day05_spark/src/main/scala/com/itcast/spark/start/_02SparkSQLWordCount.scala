package com.itcast.spark.start

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用SparkSQL进行词频统计WordCount：SQL
 */
object _02SparkSQLWordCount {

  def main(args: Array[String]): Unit = {

    // 使用建造设设计模式，创建SparkSession实例对象
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

    //导入隐式转换的包
    import spark.implicits._

    // TODO: 使用SparkSession加载数据
    val inputDS: Dataset[String] = spark.read.textFile("datas/wordcount.data")

    /*
      table: words , column: value
          SQL: SELECT value, COUNT(1) AS count  FROM words GROUP BY value
     */
    // step 1. 将Dataset或DataFrame注册为临时视图
    inputDS.createOrReplaceTempView("temp_view")
    // step 2. 编写SQL并执行
    val resultDF: DataFrame = spark.sql(
      """
        |WITH a AS
        |(SELECT explode(split(trim(value),"\\s+")) word FROM temp_view)
        |SELECT a.word,count(*) total
        |FROM a
        |GROUP BY a.word
        |ORDER BY total DESC
        |""".stripMargin)

    resultDF.show(10,truncate = 10)
    // 应用结束，关闭资源
    spark.stop()
  }

}
