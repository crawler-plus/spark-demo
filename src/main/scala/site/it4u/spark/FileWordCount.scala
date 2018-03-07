package site.it4u.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用sparkstreaming 处理文件系统数据（local/hdfs)
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {
    // 由于不需要使用receiver，所以local[1]或者n都可以
    val sparkConf = new SparkConf()
      .setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 监控一个文件系统
    val lines = ssc.textFileStream("d:/ss/")
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
