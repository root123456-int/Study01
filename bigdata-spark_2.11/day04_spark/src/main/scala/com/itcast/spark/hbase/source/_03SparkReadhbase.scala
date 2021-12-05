package com.itcast.spark.hbase.source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author by FLX
 * @date 2021/6/17 0017 10:11.
 */
object _03SparkReadhbase {
  def main(args: Array[String]): Unit = {
    val sc = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

        // TODO: 设置使用Kryo 序列化方式
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // TODO: 注册序列化的数据类型
        .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))



      SparkContext.getOrCreate(sparkConf)
    }

    //TODO:从Hbase表读取数据,调用RDD方法:newAPIHadoopRDD

    val conf: Configuration = HBaseConfiguration.create()


    //设置连接Zookeeper
    conf.set("hbase.zookeeper.quorum", "node1.itcast.cn")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")

    //设置将数据保存的Hbase表的名称
    conf.set(TableInputFormat.INPUT_TABLE, "htb_wordcount")

    /*
		  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
		      conf: Configuration = hadoopConfiguration,
		      fClass: Class[F],
		      kClass: Class[K],
		      vClass: Class[V]
		  ): RDD[(K, V)]
		 */

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf, //Hbase Client配置信息及表名称
      classOf[TableInputFormat], //采用InputFormat格式读取数据
      classOf[ImmutableBytesWritable], //Key 类型
      classOf[Result] //Value类型,封装每条数据
    )
    //println(s"count = ${hbaseRDD.count()}")

    //获取每条数据具体值
    //take()是将Task中的数据传递给driver,需要经过网络传输
    //ImmutableBytesWritable不支持Java序列化
    hbaseRDD.take(6).foreach {
      case (rowKey, result) =>
        println(s"RowKey = ${Bytes.toString(result.getRow)}")
        val cells: Array[Cell] = result.rawCells()
        cells.foreach {
          cell =>
            val family: String = Bytes.toString(CellUtil.cloneFamily(cell))
            val column: String = Bytes.toString(CellUtil.cloneQualifier(cell))
            val value: String = Bytes.toString(CellUtil.cloneValue(cell))
            println(s"\t${family}:${column} = ${value},version = ${cell.getTimestamp}")
        }
    }


    sc.stop()
  }
}
