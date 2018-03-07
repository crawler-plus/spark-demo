package site.it4u.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming整合Flume的第二种方式（拉取模式）一定要先启动flume
  */
object FlumePullWordCount {

  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      System.err.println("Usage: FlumePullWordCount <hostname> <port>")
      System.exit(1)
    }
    val Array(hostname, port) = args

    val sparkConf = new SparkConf()//.setMaster("local[2]")
      //.setAppName("FlumePullWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 使用sparkstreaming整合flume（拉取模式）
    val flumeStream = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)
    flumeStream.map(x=> new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
