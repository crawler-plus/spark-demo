package site.it4u.spark.project.domain

/**
  * 网站各操作系统访问次数实体类
  * operationSystemType: 对应的hbase中的rowkey
  * count：访问总数
  */
case class OperationSystemCount(operationSystemType: String, count: Long)
