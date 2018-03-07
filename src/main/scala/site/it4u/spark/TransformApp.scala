package site.it4u.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单去除
  */
object TransformApp {

  def main(args: Array[String]): Unit = {
    // 必须要设置成大于1的，否则无法处理
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    // 创建streamingContext需要两个参数，一个是sparkconf，一个是Seconds
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 构建黑名单
    val blacks = List("zs", "ls")
    // 将黑名单转换成RDD
    val blackRDD = ssc.sparkContext.parallelize(blacks)
      .map(x=>(x, true))
    val lines = ssc.socketTextStream("hadoop", 6790)
    // 根据名字拆分（一个名字和一个完整的信息）
    val clickLog = lines.map(x=>(x.split(",")(1), x))
      .transform(rdd=>{
        rdd.leftOuterJoin(blackRDD)
          .filter(x=>x._2._2.getOrElse(false) != true)
          .map(x=>x._2._1)
      })
    clickLog.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
