package com.itcast.spark.review.source

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author by FLX
 * @date 2021/11/5 0005 14:57.
 */
object SparkMySQLTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    //1. 简洁版

    /*
    def jdbc(
      url: String,
      table: String,
      properties: Properties
    ): DataFrame

    TODO:将表的数据加载进DataFrame中,仅仅只有1个partitions,数据量不能太大
     */
    val props: Properties = new Properties() {
      {
        put("user", "root")
        put("password", "123456")
        //put("driver", "com.mysql.cj.jdbc.Driver")
        put("driver", "com.mysql.cj.jdbc.Driver")
      }
    }
    val empDF: DataFrame = spark.read.jdbc(
      "jdbc:mysql://192.168.88.100:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
      "db_test.emp",
      props
    )

    empDF.printSchema()
    empDF.show(10, false)

    println("======================")
    //2. 标准版

    val table: String =
      """
        |(
        |SELECT
        | a.deptname,
        | b.ename,
        | b.hiredate,
        | b.sal
        |FROM db_test.dept a
        | LEFT JOIN db_test.emp b
        | ON a.deptno = b.deptno
        | ) AS tmp
        |""".stripMargin

    val df: DataFrame = spark
      .read
      .format("jdbc")
      .option("user", "root")
      .option("password", "123456")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.88.100:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
      .option("dbtable", table)
      .load()
    df.printSchema()
    df.show(10,false)


    spark.stop()
  }

}
