package site.it4u.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用sparkstreaming完成统计,并将结果写入mysql中
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val lines = ssc.socketTextStream("hadoop", 6790)
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    result.print()
    // 将结果写入mysql（存在的问题：对于已有的数据没有更新，只是插入）
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取mysql的链接
    * @return
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "12345678")
  }
}
