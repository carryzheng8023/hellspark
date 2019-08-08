package xin.carryzheng.spark.dataset

import org.apache.spark.sql.SparkSession

/**
  *
  * @author zhengxin
  *         2019-08-08 16:39:43
  */
object DataSetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataSetApp").master("local[2]").getOrCreate()

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/zhengxin/tmp/people.csv")
    df.show()

    import spark.implicits._
    val ds = df.as[Emp]

    ds.map(line => line.name).show()

    spark.stop()
  }

  case class Emp(name: String, age: Int, job: String)

}
