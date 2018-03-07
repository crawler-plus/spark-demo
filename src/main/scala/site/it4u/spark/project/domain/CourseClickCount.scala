package site.it4u.spark.project.domain

/**
  * 实战课程点击数实体类
  * day_article: 对应的hbase中的rowkey
  * click_count：访问总数
  */
case class CourseClickCount(day_article: String, click_count: Long)
