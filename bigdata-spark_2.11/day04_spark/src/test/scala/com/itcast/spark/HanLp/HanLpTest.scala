package com.itcast.spark.HanLp

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer

/**
 * @author by FLX
 * @date 2021/6/16 0016 20:44.
 */
object HanLpTest {
  def main(args: Array[String]): Unit = {
    //入门Demo
    val terms: util.List[Term] = HanLP.segment("火辣辣和货拉拉哎呦喂")

    //TODO:将Java集合对象转化为Scala集合对象
    import scala.collection.JavaConverters._
    terms.asScala.foreach(term => println(term.word))

    //标准分词
    val terms1: util.List[Term] = StandardTokenizer.segment("房价++端午++输液")
    println(terms1.asScala.map(_.word.replaceAll("\\s+","\\s+")).mkString("\n"))


  }

}
