package site.it4u.spark.project.dao

import site.it4u.project.utils.RedisUtil
import site.it4u.spark.project.domain.CourseClickCount

import scala.collection.mutable.ListBuffer

/**
  * 实战点击数访问层
  */
object ArticleClickCountDAO {

  /**
    * 保存数据到Redis
    * @param list
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {
    val jedis = RedisUtil.getJedis
    for(ele <- list) {
      // 按照日期对每天的每个文章的点击数累加,组成hash
      jedis.hincrBy(ele.day_article.split("_")(0), ele.day_article.split("_")(1), ele.click_count)
      // 每天所有文章的总点击量累加
      jedis.incrBy("totalArticleClickCount", ele.click_count)
    }
    RedisUtil.returnResource(jedis)
  }
}
