package xin.carryzheng.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCountApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0))

    val wc = textFile.flatMap(lines => lines.split(" ").map((_, 1))).reduceByKey(_ + _)

    val sorted = wc.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))

    sorted.saveAsTextFile(args(1))

    sc.stop()
  }

}
