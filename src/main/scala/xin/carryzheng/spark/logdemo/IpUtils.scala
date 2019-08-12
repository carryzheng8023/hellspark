package xin.carryzheng.spark.logdemo

import com.ggstar.util.ip.IpHelper

/**
  * IP解析工具类
  * https://github.com/wzhe06/ipdatabase.git
  * @author zhengxin
  *         2019-08-12 16:52:00
  */
object IpUtils {

  def getCity(ip:String) = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("58.30.15.255"))
  }

}
