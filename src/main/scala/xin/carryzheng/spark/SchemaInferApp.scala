package xin.carryzheng.spark

import org.apache.spark.sql.SparkSession

/**
  *
  * @author zhengxin
  *         2019-08-16 14:12:18
  */
object SchemaInferApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SchemaInferApp").master("local[2]").getOrCreate()

    val df = spark.read.format("json").load("file:///Users/zhengxin/tmp/people.json")

    df.printSchema()
    df.show()

    spark.stop()
  }



}
