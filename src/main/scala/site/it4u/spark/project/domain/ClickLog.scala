package site.it4u.spark.project.domain

/**
  * 清洗后的日志信息
  * ip: ip地址
  * time: 访问时间
  * articleId 访问文章编号
  * statusCode 状态码
  * urlType 访问url类型
  * userAgent userAgent类型
  * terminalType 电脑端还是移动端（电脑端为0, 移动端为1）
  * operationSystemType 操作系统（0：others，Chrome：1，Safari：2，IE：3，Firefox：4，Opera：5，QQ：6，360:7，UC：8）
  * cityName: 访问地市
  */
case class ClickLog(ip: String, time:String, articleId:Int, statusCode:Int, urlType: Int, userAgent: String, terminalType: Int, operationSystemType: String, cityName: String)