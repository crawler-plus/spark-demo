package site.it4u.spark.project.spark

import eu.bitwalker.useragentutils.UserAgent
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import site.it4u.spark.project.dao._
import site.it4u.spark.project.domain._
import site.it4u.spark.project.utils.DateUtils

import scala.collection.mutable.ListBuffer

/**
  * 使用spark streaming处理kafka过来的数据
  */
object StatStreamingApp {

  def main(args: Array[String]): Unit = {
    if(args.length != 4) {
      println("usages: StatStreamingApp <zkQuorum> <groupId> <topicMap> <numThreads>")
    }
    var Array(zkQuorum, groupId, topics, numThreads) = args
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)
    // 数据清洗
    val logs = messages.map(_._2)
    // 过滤出所有访问的data
    val cleanData = logs.map(line => {
      // 得到每行的数据，用逗号分隔
      val infos = line.split(",")
      // 得到url
      val url = infos(2).split(" ")(1)
      // 终端类型
      var terminalType = 0
      // 浏览器类型
      var operationSystemType = ""
      // 得到userAgent
      val userAgent = infos(4)
      // 首先判断userAgent是电脑端还是移动端
      // 是移动端
      if(userAgent.contains("Android")
        || userAgent.contains("webOS")
        || userAgent.contains("iPhone")
        || userAgent.contains("iPad")
        || userAgent.contains("iPod")
        || userAgent.contains("BlackBerry")
        || userAgent.contains("Windows Phone")) {
        terminalType = 1
      }
      // 其次判断userAgent的浏览器
      val ua = UserAgent.parseUserAgentString(userAgent)
      operationSystemType = ua.getBrowser.getName
      // 定义文章编号为0
      var articleId = 0
      // 定义url类型为0
      var urlType = 0
      // 如果访问首页
      if(url == "/") {
        urlType = 1;
      }
      // 如果访问文章详情页
      if(url.startsWith("/article")) {
        // 取到文章编号
        articleId = url.split("/")(2).toInt
        urlType = 2;
      }
      // 如果访问about页面
      if(url.startsWith("/about")) {
        urlType = 3;
      }
      // 如果访问统计页面
      if(url.startsWith("/stat")) {
        urlType = 4;
      }
      // 给clicklog赋值
      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), articleId.toInt, infos(3).toInt, urlType.toInt, infos(4), terminalType, operationSystemType)
    }).filter(cl => cl.urlType != 0)

    // 过滤出是文章的data
    val articleCleanData = cleanData.filter(cl => cl.articleId != 0)

    /**
      * 打印到控制台
      */
    cleanData.print()
    articleCleanData.print()

    // 统计今天到现在为止实战课程的访问量
    articleCleanData.map(x => {
      // HBase rowkey设计： 20171111_88
      (x.time.substring(0, 8) + "_" + x.articleId, 1)
    }).reduceByKey(_+_)// 每个文章按照日期加1
      .foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })
        ArticleClickCountDAO.save(list)
      })
    })

    // 统计到现在为止网站访问人次
    cleanData.map(x => {
      // HBase rowkey设计：
      ("totalClickCount", 1)
    }).reduceByKey(_+_)// 按照rowkey+1
      .foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[AccessTimesCount]
        partitionRecords.foreach(pair => {
          list.append(AccessTimesCount(pair._1, pair._2))
        })
        AccessTimesCountDAO.save(list)
      })
    })

    // 统计今天到现在为止网站电脑端和手机端访问次数
    cleanData.map(x => {
      // HBase rowkey设计： 20171111_88
      (""+ x.terminalType, 1)
    }).reduceByKey(_+_)// 加1
      .foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[TerminalCount]
        partitionRecords.foreach(pair => {
          list.append(TerminalCount(pair._1, pair._2))
        })
        TerminalDAO.save(list)
      })
    })

    // 统计今天到现在为止网站各操作系统访问次数
    cleanData.map(x => {
      // HBase rowkey设计：
      ("" + x.operationSystemType, 1)
    }).reduceByKey(_+_)// 加1
      .foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[OperationSystemCount]
        partitionRecords.foreach(pair => {
          list.append(OperationSystemCount(pair._1, pair._2))
        })
        OperationSystemDAO.save(list)
      })
    })

    // 执行
    ssc.start()
    ssc.awaitTermination()
  }
}