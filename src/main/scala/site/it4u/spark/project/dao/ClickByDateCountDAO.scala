package site.it4u.spark.project.dao

import site.it4u.project.utils.RedisUtil
import site.it4u.spark.project.domain.ClickByDateCount

import scala.collection.mutable.ListBuffer

/**
  * 根据日期计算访问次数访问层
  */
object ClickByDateCountDAO {

  /**
    * 保存数据到Redis
    * @param list
    */
  def save(list: ListBuffer[ClickByDateCount]): Unit = {
    val jedis = RedisUtil.getJedis
    for(ele <- list) {
      // 访问次数累加
      jedis.hincrBy("clickByDateCount", ele.date, ele.count)
    }
    RedisUtil.returnResource(jedis)
  }
}
