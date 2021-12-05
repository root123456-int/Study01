package com.itcast.spark.hbase.sink


/**
 * @author by FLX
 * @date 2021/6/17 0017 8:29.
 */
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将RDD数据保存至HBase表中
 */
object _02SparkWriteHbase01 {

  def main(args: Array[String]): Unit = {
    // 1. 在Spark 应用程序中，入口为：SparkContext，必须创建实例对象，加载数据和调度程序执行
    val sc: SparkContext = {
      // 创建SparkConf对象，设置应用相关信息，比如名称和master
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      // 构建SparkContext实例对象，传递SparkConf
      SparkContext.getOrCreate(sparkConf)
    }

    // 2. 第一步、从LocalFS读取文件数据，sc.textFile方法，将数据封装到RDD中
    val inputRDD: RDD[String] = sc.textFile("datas/wordcount.data")

    // 3. 第二步、调用RDD中高阶函数，进行处理转换处理，函数：flapMap、map和reduceByKey
    val resultRDD: RDD[(String, Int)] = inputRDD
      // a. 过滤
      .filter(line => null != line && line.trim.length > 0 )
      // b. 对每行数据按照分割符分割
      .flatMap(line => line.trim.split("\\s+"))
      // c. 将每个单词转换为二元组，表示出现一次
      .map(word => (word ,1))
      // d. 分组聚合
      .reduceByKey((temp, item) => temp + item)

    // 4. 第三步、将最终处理结果RDD保存到HDFS或打印控制台
    /*
      (hive,6)
      (spark,11)
      (mapreduce,4)
      (hadoop,3)
      (sql,2)
      (hdfs,2)
     */
    //resultRDD.foreach(tuple => println(tuple))


    // TODO: step 1. 转换RDD为RDD[(RowKey, Put)]
    /*
      * HBase表的设计：
        * 表的名称：htb_wordcount
        * Rowkey: word
        * 列簇: info
        * 字段名称： count
      create 'htb_wordcount', 'info'
     */
    val putRDD: RDD[(ImmutableBytesWritable, Put)] = resultRDD.map{case (word, count) =>
      // step1、构建RowKey对象
      val rowKey: ImmutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(word))
      // step2、创建Put对象
      val put: Put = new Put(rowKey.get())
      // 设置列，此处就是count
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count.toString))
      // step3、返回二元组
      (rowKey, put)
    }

    // TODO: step2. 调用RDD中saveAsNewAPIHadoopFile保存数据
    val conf: Configuration = HBaseConfiguration.create()
    // 设置连接Zookeeper属性
    conf.set("hbase.zookeeper.quorum", "node1.itcast.cn")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    // 设置将数据保存的HBase表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_wordcount")
    /*
      def saveAsNewAPIHadoopFile(
          path: String,
          keyClass: Class[_],
          valueClass: Class[_],
          outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
          conf: Configuration = self.context.hadoopConfiguration
      ): Unit
     */
    putRDD.saveAsNewAPIHadoopFile(
      "datas/output-hbase/10001", //
      classOf[ImmutableBytesWritable], // Key类型
      classOf[Put], // Value类型
      classOf[TableOutputFormat[ImmutableBytesWritable]], // OutputFormat类型
      conf // 表示连接HBase表时基本信息设置，就主要就是Zookeeper地址
    )

    // 5. 当应用运行结束以后，关闭资源
    sc.stop()
  }

}
