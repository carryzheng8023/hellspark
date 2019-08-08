package xin.carryzheng.spark.dataframe

import org.apache.spark.sql.SparkSession

/**
  * DataFrame API基本操作
  *
  * @author zhengxin
  *         2019-08-08 10:19:41
  */
object DataFrameApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataFrameApp").master("local[2]").getOrCreate()

    // 将json文件加载成一个dataframe
    val df_people = spark.read.format("json").load("file:///Users/zhengxin/tmp/people.json")

    // 输出dataframe对应schema信息
    df_people.printSchema()

    // 查询数据的前20条记录
    df_people.show()

    // 查询某列所有的数据
    df_people.select("name").show()

    // 查询某几列的数据，并对列进行计算
    df_people.select(df_people.col("name"), (df_people.col("age") + 10)).as("age").show()

    // 根据某一列值进行过滤
    df_people.filter(df_people.col("age") > 19).show()

    // 根据某列进行分组，然后再进行聚合操作
    df_people.groupBy("age").count().show()

    // 创建临时表，用sql进行查询
    df_people.createOrReplaceTempView("people")
    spark.sql("select * from people").show()

    spark.stop()

  }

}
