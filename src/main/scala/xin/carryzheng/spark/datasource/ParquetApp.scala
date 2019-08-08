package xin.carryzheng.spark.datasource

import org.apache.spark.sql.SparkSession

/**
  * 处理parquet格式数据
  * http://spark.apache.org/docs/latest/sql-data-sources-parquet.html
  * @author zhengxin
  *         2019-08-08 17:22:59
  */
object ParquetApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    val df = spark.read.format("parquet").load("file:///Users/zhengxin/tmp/users.parquet")

    df.printSchema()
    df.show()

    df.select("name", "favorite_color").show()

    //写出
//    df.select("name", "favorite_color").write.format("json").save("file:///Users/zhengxin/tmp/")

    spark.stop()
  }

  /**
    * spark sql
    * */
//  CREATE TEMPORARY VIEW parquetTable
//    USING org.apache.spark.sql.parquet
//  OPTIONS (
//    path "examples/src/main/resources/people.parquet"
//  )
//
//  SELECT * FROM parquetTable

}
