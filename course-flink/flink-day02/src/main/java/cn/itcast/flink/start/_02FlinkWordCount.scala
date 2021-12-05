package cn.itcast.flink.start

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * 使用Scala语言实现词频统计：WordCount
 */
object _02FlinkWordCount {

  def main(args: Array[String]): Unit = {
    // 1. 执行环境-env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


    // 2. 数据源-source
    val inputDS: DataSet[String] = env.readTextFile("datas/wordcount.data")

    // 3. 数据转换-transformation
    val resultDS: DataSet[(String, Int)] = inputDS
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .map(word => word -> 1)
      .groupBy(0)
      .sum(1)
      .sortPartition(1, Order.DESCENDING)
      .setParallelism(1)

    // 4. 数据终端-sink
    resultDS.printToErr()

    // 5. 触发执行-execute
  }

}
