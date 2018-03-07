package site.it4u.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming对接kafka方式1（receiver）
  */
object KafkaStreamingApp {

  def main(args: Array[String]): Unit = {
    if(args.length != 4) {
      System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount")
       .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // 对接kafka
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    messages.map(_._2).count().print()
    ssc.start()
    ssc.awaitTermination()
  }
}
