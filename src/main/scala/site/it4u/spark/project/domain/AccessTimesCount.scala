package site.it4u.spark.project.domain

/**
  * 访问网站人次实体类
  * clickKey: key
  * count：访问总数
  */
case class AccessTimesCount(clickKey: String, count: Long)
