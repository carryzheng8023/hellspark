package xin.carryzheng.spark.logdemo

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


/**
  * TopN统计spark作业
  *
  * @author zhengxin
  *         2019-08-12 17:13:29
  */
object TopNStatJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("/Users/zhengxin/tmp/access_clean")

    //    accessDF.printSchema()
    //    accessDF.show(false)

    // 最受欢迎的TopN课程
//    videoAccessTopNStat(spark, accessDF)

    // 按照地市进行统计TopN课程
    cityAccessTopNStat(spark, accessDF)

    spark.stop()

  }

  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame) = {

    import spark.implicits._

    val cityAccessTopNDF = accessDF.filter($"day" === "20190812" && $"cmsType" === "vedio")
      .groupBy("day","city", "cmsId")
      .agg(count("cmsId").as("times"))

//    cityAccessTopNDF.show(false)

    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number()
        .over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc))
        .as("times_rank")
    ).filter("times_rank <= 3") //Top3


    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })

        StatDAO.insertDayCityVideoAccessTopN(list)

      })
    } catch {
      case e: Exception => e.printStackTrace()
    }


  }

  /**
    * 前topN
    *
    * @author zhengxin
    * @date 2019/8/12 17:20
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame) = {

    /**
      * 使用df
      **/
    //    import spark.implicits._
    //    val videoAccessTopNDF = accessDF.filter($"day" === "20190812" && $"cmsType" === "vedio")
    //      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    //

    /**
      * 使用sql
      **/
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) times from access_logs " +
      "where day = '20190812' and cmsType = 'vedio' group by day,cmsId order by times desc")

    //    videoAccessTopNDF.show(false)

    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)

      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

}
