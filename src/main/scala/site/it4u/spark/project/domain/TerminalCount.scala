package site.it4u.spark.project.domain

/**
  * 网站电脑端和手机端访问次数实体类
  * terminalType: key
  * count：访问总数
  */
case class TerminalCount(terminalType: String, count: Long)
