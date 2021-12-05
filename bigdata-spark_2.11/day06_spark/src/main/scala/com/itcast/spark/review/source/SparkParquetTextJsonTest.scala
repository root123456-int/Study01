package com.itcast.spark.review.source

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author by FLX
 * @date 2021/11/5 0005 13:52.
 */
object SparkParquetTextJsonTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    //datas/input/users.parquet

    val parquetDF1: DataFrame = spark.read
      .format("parquet")
      .option("path", "datas/input/users.parquet")
      .load()

    val parquetDF2: DataFrame = spark.read.parquet("datas/input/users.parquet")

    val parquetDF3: DataFrame = spark.read
      .format("parquet")
      .load("datas/input/users.parquet")



    //datas/input/people.txt
    val textDF: DataFrame = spark.read.text("datas/input/people.txt")

    val textDS: Dataset[String] = spark.read.textFile("datas/input/people.txt")


    //datas/input/people2.json

    val peopleJson1: DataFrame = spark.read.json("datas/input/people.json")

    val people2Json: DataFrame = spark.read
      .format("json")
      .option("path", "datas/input/people2.json")
      .load()

    val peopleDS: Dataset[String] = spark.read.textFile("datas/input/people2.json")

    import org.apache.spark.sql.functions._
    val peopleDF: DataFrame = peopleDS.select(
      get_json_object($"value", "$.name").as("name"),
      get_json_object($"value", "$.age").as("age")
    )
      .orderBy($"age")

    peopleDF.printSchema()
    peopleDF.show(2,false)


    spark.stop()
  }
}
