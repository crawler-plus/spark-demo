package site.it4u.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用sparkstreaming完成有状态的统计
  */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 如果使用了stateful的算子，必须要设置checkpoint
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("hadoop", 6790)
    val result = lines.flatMap(_.split(" ")).map((_, 1))
    //
    val state = result.updateStateByKey(updateFunction)
    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 把当前的数据去更新已有的或者是老的数据
    * @param currentValues 当前的
    * @param preValues 老的
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }
}
