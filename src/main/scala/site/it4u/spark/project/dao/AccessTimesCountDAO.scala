package site.it4u.spark.project.dao

import site.it4u.project.utils.RedisUtil
import site.it4u.spark.project.domain.AccessTimesCount

import scala.collection.mutable.ListBuffer

/**
  * 访问网站人次访问层
  */
object AccessTimesCountDAO {

  /**
    * 保存数据到Redis
    * @param list
    */
  def save(list: ListBuffer[AccessTimesCount]): Unit = {
    val jedis = RedisUtil.getJedis
    for(ele <- list) {
      jedis.incrBy(ele.clickKey, ele.count)
    }
    RedisUtil.returnResource(jedis)
  }
}
