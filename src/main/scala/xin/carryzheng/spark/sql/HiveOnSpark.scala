package xin.carryzheng.spark.sql

import org.apache.spark.sql.SparkSession
/**
  *
  * @author zhengxin
  *         2019-08-06 17:43:54
  */
object HiveOnSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport()
      .appName("hiveOnSpark").master("local[2]").getOrCreate()

//    spark.sql("show databases").collect().foreach(println)
    //idea运行需要将hive-site.xml添加到resources目录下
    spark.sql("select * from default.emp").show()
  }

}
