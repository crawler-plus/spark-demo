package site.it4u.spark.project.dao

import site.it4u.project.utils.RedisUtil
import site.it4u.spark.project.domain.OperationSystemCount

import scala.collection.mutable.ListBuffer

/**
  * 网站各操作系统访问次数访问层
  */
object OperationSystemDAO {

  /**
    * 保存数据到Hbase
    * @param list
    */
  def save(list: ListBuffer[OperationSystemCount]): Unit = {
    val jedis = RedisUtil.getJedis
    for(ele <- list) {
      // 对每一个电脑端和手机端访问次数累加
      jedis.hincrBy("operationSystemType", ele.operationSystemType, ele.count)
    }
    RedisUtil.returnResource(jedis)
  }
}
