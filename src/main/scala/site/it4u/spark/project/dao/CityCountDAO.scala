package site.it4u.spark.project.dao

import site.it4u.project.utils.RedisUtil
import site.it4u.spark.project.domain.CityCount

import scala.collection.mutable.ListBuffer

/**
  * 地市次数访问层
  */
object CityCountDAO {

  /**
    * 保存数据到Redis
    * @param list
    */
  def save(list: ListBuffer[CityCount]): Unit = {
    val jedis = RedisUtil.getJedis
    for(ele <- list) {
      // 对每一个城市访问次数累加
      jedis.hincrBy("cityCount", ele.cityName, ele.count)
    }
    RedisUtil.returnResource(jedis)
  }
}
