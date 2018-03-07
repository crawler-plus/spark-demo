package site.it4u.spark.project.dao

import site.it4u.project.utils.RedisUtil
import site.it4u.spark.project.domain.TerminalCount

import scala.collection.mutable.ListBuffer

/**
  * 网站电脑端和手机端访问次数访问层
  */
object TerminalDAO {

  /**
    * 保存数据到Redis
    * @param list
    */
  def save(list: ListBuffer[TerminalCount]): Unit = {
    val jedis = RedisUtil.getJedis
    for(ele <- list) {
      // 对每一个电脑端和手机端访问次数累加
      jedis.hincrBy("terminalType", ele.terminalType, ele.count)
    }
    RedisUtil.returnResource(jedis)
  }
}
