package site.it4u.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming 处理socket数据
  */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {
    // 必须要设置成大于1的，否则无法处理
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    // 创建streamingContext需要两个参数，一个是sparkconf，一个是Seconds
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val lines = ssc.socketTextStream("hadoop", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
