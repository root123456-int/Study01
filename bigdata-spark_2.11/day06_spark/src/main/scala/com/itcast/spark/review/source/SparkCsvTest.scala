package com.itcast.spark.review.source

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author by FLX
 * @date 2021/11/5 0005 14:21.
 */
object SparkCsvTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    val uDF: DataFrame = spark
      .read
      .format("csv")
      .option("sep", "\\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("datas/input/ml-100k/u.dat")

    uDF.printSchema()
    uDF.show(10, false)

    println("===========================================")
    val schema: StructType = StructType(
      StructField("userID", StringType, false) ::
        StructField("movieID", StringType, false) ::
        StructField("rating", DoubleType, true) ::
        StructField("timestamp", LongType, true) :: Nil
    )

    val ratingDF: DataFrame = spark.read
      .format("csv")
      .option("sep", "\\t")
      .schema(schema)
      .load("datas/input/ml-100k/u.data")

    ratingDF.printSchema()
    ratingDF.show(10,false)


    spark.stop()
  }

}
