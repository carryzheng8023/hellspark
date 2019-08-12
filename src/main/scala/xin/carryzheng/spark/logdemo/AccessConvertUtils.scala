package xin.carryzheng.spark.logdemo

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换工具类
  *
  * @author zhengxin
  *         2019-08-12 16:28:15
  */
object AccessConvertUtils {

  //定义输出的字段
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  /**
    * 根据输入的每一行信息转化成输出的样式
    *
    * @author zhengxin
    * @date 2019/8/12 16:32
    */
  def parseLog(log: String) = {

    try {
      val splits = log.split("\t")
      val url = splits(1)
      val traffic = splits(2).toLong
      var ip = splits(3)

      val domain = "http://carryzheng.xin/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")

      var cmsType = ""
      var cmsId = 0L

      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city = IpUtils.getCity(ip)
      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      //Row里边的字段需要和struct中字段对应上
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)

    } catch {
      case e: Exception => Row(0)
    }


  }


}
