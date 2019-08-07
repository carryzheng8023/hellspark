package xin.carryzheng.spark

import org.apache.spark.sql.SparkSession

/**
  *
  * @author zhengxin
  *         2019-08-07 16:14:58
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

    val people = spark.read.json("file:///Users/zhengxin/tmp/people.json")
    people.show()

    spark.stop()

  }

}
