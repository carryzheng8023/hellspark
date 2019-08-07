package xin.carryzheng.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object PageViewsApp {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val pageViews = sc.textFile("file:///home/hadoop/data/page_view.dat")

    pageViews.map(x => (x.split("\t")(5), 1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(10)

    sc.stop()

  }

}
