package site.it4u.spark.project.domain

/**
  * 根据日期计算访问数量
  * terminalType: key
  * count：访问总数
  */
case class ClickByDateCount(date: String, count: Long)
