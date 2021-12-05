package com.itcast.spark.hbase.sink

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/17 0017 8:29.
 */
object _02SparkWriteHbase {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data")

    val resultRDD: RDD[(String, Int)] = inputRDD
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(_.trim.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)
    /*
        resultRDD.foreach(println)

        (spark,11)
        (hive,6)
        (hadoop,3)
        (mapreduce,4)
        (hdfs,2)
        (sql,2)
     */

    //TODO:写入Hbase
    //step1:将RDD转化为RDD[(RowKey,Put)]
    /*
    Hbase表的设计:
      表名称:htb_wordcount
      RowKey:word
      列族:info
      字段名称:count
      create 'htb_wordcount','info'
     */
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = resultRDD.map {
      case (word, count) =>
        //一,构建RowKey对象
        val rowKey: ImmutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(word))
        //二,构建Put对象
        val put: Put = new Put(rowKey.get())
        //设置字段的值
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count.toString))

        //三,返回二元组(Rowkey,put)
        (rowKey -> put)
    }

    //TODO:step2:调用RDD中saveAsNewAPIHadoopFile保存数据
    val conf: Configuration = HBaseConfiguration.create()
    //设置连接Zookeeper
    conf.set("hbase.zookeeper.quorum", "node1.itcast.cn")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    //设置将数据保存的Hbase表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE,"htb_wordcount")
    /*
		  def saveAsNewAPIHadoopFile(
		      path: String,
		      keyClass: Class[_],
		      valueClass: Class[_],
		      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
		      conf: Configuration = self.context.hadoopConfiguration
		  ): Unit
		 */
    putsRDD.saveAsNewAPIHadoopFile(
      "datas/output-hbase/10001", //
      classOf[ImmutableBytesWritable], // Key类型
      classOf[Put], //Value 类型
      classOf[TableOutputFormat[ImmutableBytesWritable]], //OutputFormat类型
      conf //表示连接Hbase表时基本信息设置,就主要时Zookeeper地址
    )

    //关闭资源
    sc.stop()

  }
}
