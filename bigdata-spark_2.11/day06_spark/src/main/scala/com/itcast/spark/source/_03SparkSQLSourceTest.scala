package com.itcast.spark.source

import java.util.Properties

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SparkSQL外部数据源，加载CSV和MySQL表的数据
 */
object _03SparkSQLSourceTest {

  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    import spark.implicits._

    // TODO: 1. CSV 格式数据文本文件数据 -> 依据 CSV文件首行是否是列名称，决定读取数据方式不一样的
    /*
      CSV 格式数据：
        每行数据各个字段使用逗号隔开
        也可以指的是，每行数据各个字段使用 单一 分割符 隔开数据
     */
    // 方式一：首行是列名称，数据文件u.dat
    val dfCSV: DataFrame = spark.read
      .format("csv")
      .option("sep", "\\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("datas/ml-100k/u.dat")

    dfCSV.printSchema()
    dfCSV.show(10, truncate = false)
    println("=====================================")


    // 方式二：首行不是列名，需要自定义Schema信息，数据文件u.data
    val schema: StructType = new StructType()
      .add("userid", IntegerType, nullable = true)
      .add("iterid", IntegerType, nullable = true)
      .add("rating", DoubleType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    val dfCSV01: DataFrame = spark.read
      .format("csv")
      .schema(schema)
      .option("sep", "\\t")
      .load("datas/ml-100k/u.data")

    dfCSV01.printSchema()
    dfCSV01.show(10, truncate = false)

    /* ============================================================================== */
    // TODO: 2. 读取MySQL表中数据
    /*
      ("url", "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
      ("driver", "com.mysql.cj.jdbc.Driver")
      ("user", "root")
      ("password", "123456")
      ("dbtable", table)
     */
    // 第一、简洁版格式
    val props01 = new Properties()
    props01.put("user", "root")
    props01.put("password", "123456")
    props01.put("driver", "com.mysql.cj.jdbc.Driver")

    val empDF: DataFrame = spark.read.jdbc(
      "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
      "db_test.emp",
      props01
    )

    empDF.printSchema()
    empDF.show(10,truncate = false)

    println("==============================================")

    // 第二、标准格式写
    val table: String = "(select ename,hiredate,sal,deptname from db_test.dept d join db_test.emp e on d.deptno = e.deptno) tmp"

    val resultSQL: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://node1.itcast.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", table)
      .load()

    resultSQL.printSchema()
    resultSQL.show(10,truncate = false)

    // 应用结束，关闭资源
    spark.stop()
  }

}
