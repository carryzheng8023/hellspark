package xin.carryzheng.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author zhengxin
  *         2019-08-07 15:51:03
  */
object HiveContextApp {

  def main(args: Array[String]): Unit = {

    //1. 创建相应context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("HiveContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2. 相应处理
    hiveContext.table("default.emp").show()

    //3.关闭资源
    sc.stop()


  }

}
