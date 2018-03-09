package site.it4u.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间工具类
  */
object DateUtils {

  val YYYYMMDDHHMMSS = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")
  var DATE_FORMAT = FastDateFormat.getInstance("yyyyMMdd")

  def getTime(time: String) = {
    YYYYMMDDHHMMSS.parse(time).getTime
  }

  def parseToMinute(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def parseToDate(time: String) = {
    DATE_FORMAT.format(new Date(getTime(time)))
  }
}
