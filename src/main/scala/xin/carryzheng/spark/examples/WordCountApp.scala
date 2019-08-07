package xin.carryzheng.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0))

    val wc = textFile.flatMap(lines => lines.split(" ").map((_,1))).reduceByKey(_+_)

//    wc.collect().foreach(println)

    wc.saveAsTextFile(args(1))

    sc.stop()
  }

}
