package xin.carryzheng.spark.logdemo

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间解析工具类
  *
  * SimpleDateFormat是线程不安全的
  *
  * @author zhengxin
  *         2019-08-12 14:16:33
  */
object DateUtils {



  // 输入文件日期格式
  // 12/Aug/2019:12:26:38 +0800
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  // 目标日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


  /**
    * 获取时间：yyyy-MM-dd HH:mm:ss
    *
    * @author zhengxin
    * @date 2019/8/12 14:22
    */
  def parse(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }


  /**
    *
    * 获取输入日志时间：long类型
    *
    * time：12/Aug/2019:12:26:38 +0800
    *
    * @author zhengxin
    * @date 2019/8/12 14:21
    */
  def getTime(time: String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time).getTime
    } catch {
      case e: Exception => {
        0L
      }
    }

  }

  def main(args: Array[String]): Unit = {
    println(parse("12/Aug/2019:12:26:38 +0800"))
  }

}
