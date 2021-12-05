package cn.itcast.spark.kafka.source

import scala.util.Random

/**
 * @author by FLX
 * @date 2021/6/23 0023 16:42.
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    val random = new Random()
    val callIn: String = "1890000%04d".format(random.nextInt(10000))

    println(callIn)
  }
}
