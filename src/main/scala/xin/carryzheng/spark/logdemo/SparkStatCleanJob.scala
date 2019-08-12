package xin.carryzheng.spark.logdemo

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 日志数据清洗
  *
  * @author zhengxin
  *         2019-08-12 15:53:46
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///Users/zhengxin/tmp/access_format.log")

//    accessRDD.take(10).foreach(println)

    // RDD -> DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtils.parseLog(x)), AccessConvertUtils.struct)

//    accessDF.printSchema()
//    accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day")
        .save("file:///Users/zhengxin/tmp/access_clean")

    spark.stop()

  }

}
