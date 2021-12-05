package cn.itcast.spark.test01

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * @author by FLX
 * @date 2021/11/8 0008 11:20.
 */
object StructuredStreamingFileSourceTest01 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._

    val schema: StructType = StructType(
      StructField("name", StringType, nullable = true) ::
        StructField("age", IntegerType, nullable = true) ::
        StructField("hobby", StringType, nullable = true)
        :: Nil
    )
    //    .add("name",StringType,null)
    //    .add("age",IntegerType,null)
    //    .add("hobby",StringType,null)

    val inputStreamDF: DataFrame = spark.readStream
      .format("csv")
      .schema(schema)
      .option("sep", ";")
      .load("file:///C:/Develop/Tool/IntelliJ_IDEA_2020.2/IdeaProjiect/bigdata-spark_2.11/datas/input/csv")

    val resultStreamDF: DataFrame = inputStreamDF
      .where($"age".lt(25))
      .groupBy($"hobby")
      .count()
      .orderBy($"count".desc)

    val query: StreamingQuery = resultStreamDF.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
    query.stop()

  }

}
