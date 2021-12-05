package com.itcast

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.{After, Before, Test}

/**
 *
 * @author by syf
 * @ctime 2021-08-09 16:47:17
 *
 */
class Demo {
  var spark: SparkSession = _
  var pairRDD: RDD[(String, Int)] = _

  @Before
  def before(): Unit = {

    spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

    pairRDD = spark.sparkContext.parallelize(Seq("a" -> 1,"a"->1, "b" -> 2, "a" -> 1))
  }

  @Test
  def testReduceByKey(): Unit = {
//func: (V, V) => V
    val value: RDD[(String, Int)] = pairRDD.reduceByKey(_ + _)

    // "a"->1    1 1 2+2  4+3  7
    // "a"->1

    //"a"->2

    //"b"->2
    //"a"->1


    //"001"->"hello"
    //"001"->"cat"
    //"001"->"mouse"
    //"001"->"stream"


    //"a"->3
    //


// 1G
// 1G
    //100M
    //100M

//      reparttion



  }

  @Test
  def testGroupByKey(): Unit = {

    pairRDD
      .groupByKey()
      .map(e => e._1 -> e._2.sum)
      .foreach(println)

  }

  @Test
  def testAggregate(): Unit = {
//    val sum: Int = pairRDD
//      .values
//      .aggregate(0)((u, v) => u + v, _ + _))

 //  )

  }
@Test
  def testAggregateByKey(): Unit = {
    pairRDD.aggregateByKey(0)((u, v) => u + v, _ + _).foreach(println)
  }

  @Test
  def testTreeAggregate(): Unit = {
    val sum: Int = pairRDD.values.treeAggregate(0)((u, v) => u + v, _ + _, 3)


    // 1   a->1   a->1          2  a->1   a->1         3  a->1 b->1   4  a->1  b->1

    //  a->2
    //
    //  a->2                   a->2             a->1 b->1
    //  a->4    a->3  b->1


    // a->7 b->1
    println(sum)

  }

  @Test
  def testGroupBy(): Unit = {
    val pairDF: DataFrame = spark.createDataFrame(pairRDD).toDF("k", "v")

    val resultDF: DataFrame = pairDF
      .groupBy("k")
      .agg(sum("v"))
      .filter(col("k")===("a"))
    resultDF
      .explain()

    resultDF
      .show()
  }

  @After
  def after(): Unit = {
    spark.close()
  }

}
