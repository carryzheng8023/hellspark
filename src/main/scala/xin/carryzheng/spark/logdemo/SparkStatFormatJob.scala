package xin.carryzheng.spark.logdemo

import org.apache.spark.sql.SparkSession

/**
  *
  * @author zhengxin
  *         2019-08-12 12:28:46
  */
object SparkStatFormatJob {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob")
      .master("local[2]").getOrCreate()


    val access = spark.sparkContext.textFile("file:///Users/zhengxin/tmp/access.log")

    //        access.take(10).foreach(println)

    access.map(line => {
      val splits = line.split("\t")
      val time = splits(0)
      val url = splits(1)
      val traffic = splits(2)
      val ip = splits(3)
      //      (DateUtils.parse(time), url, traffic, ip)
      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("file:///Users/zhengxin/tmp/access_log_output/")

    spark.stop()
  }

}
