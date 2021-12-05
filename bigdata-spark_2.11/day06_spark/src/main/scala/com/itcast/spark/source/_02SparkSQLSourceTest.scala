package com.itcast.spark.source

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * SparkSQL 外部数据源，加载parquet列式数据，JSON格式和text文本数据
 */
object _02SparkSQLSourceTest {


  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._

    // TODO 1. parquet列式存储数据

    //format方式加载
    val df1: DataFrame = spark.read
      .format("parquet")
      .option("path", "datas/resources/users.parquet")
      .load()
    df1.show(10, truncate = false)
    println("=================================================")
    //parquet方式加载
    val df2: DataFrame = spark.read
      .parquet("datas/resources/users.parquet")
    df2.show(10, truncate = false)
    println("=================================================")
    //load方式加载，在SparkSQL中，当加载读取文件数据时，如果不指定格式，默认是parquet格式数据
    val df3: DataFrame = spark.read
      .load("datas/resources/users.parquet")
    df3.show(10, truncate = false)

    /* =========================================================================== */
    // TODO: 2. 文本数据加载，text -> DataFrame   textFile -> Dataset

    val dfTxt: DataFrame = spark.read
      .text("datas/resources/people.txt")
    dfTxt.show(10, truncate = false)

    println("=======================================================")

    val dsTxt: Dataset[String] = spark.read
      .textFile("datas/resources/people.txt")
    dsTxt.show(10, truncate = false)

    /* =========================================================================== */
    // TODO: 3. 读取JSON格式数据，自动解析，生成Schema信息
    val dfJson: DataFrame = spark.read
      .json("datas/resources/people.json")
    dfJson
      .select($"name")
      .show(10, truncate = false)

    println("=============================================")


    /* =========================================================================== */
    // TODO: 实际开发中，针对JSON格式文本数据，直接使用text/textFile读取，然后解析提取其中字段信息
    /*
      {"name":"Andy", "salary":30}   - value: String
            | 解析JSON格式，提取字段
        name: String, -> Andy
        salary : Int, -> 30
     */
    import org.apache.spark.sql.functions.get_json_object
    val dsJson01: Dataset[String] = spark.read
      .textFile("datas/resources/people.json")
    val dfJson03: DataFrame = dsJson01
      .select(
        get_json_object($"value", "$.name").as("name"),
        get_json_object($"value", "$.age").cast(IntegerType).as("age")
      )
    dfJson03.printSchema()
    dfJson03.show(10, truncate = false)

    // 应用结束，关闭资源
    spark.stop()
  }

}
