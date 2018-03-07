package site.it4u.spark.project.domain

/**
  * 访问网站人次实体类
  * clickKey: 对应的hbase中的rowkey
  * count：访问总数
  */
case class AccessTimesCount(clickKey: String, count: Long)
