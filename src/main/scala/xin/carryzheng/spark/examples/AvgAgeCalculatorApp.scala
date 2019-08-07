package xin.carryzheng.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

object AvgAgeCalculatorApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val dataFile = sc.textFile("file:////home/hadoop/data/sample_age_data.txt")

    val ageData = dataFile.map(x => x.split(" ")(1))

    val count = dataFile.count()

    val totalAge = ageData.map(age => age.toInt).reduce(_+_)

    val avgAge = totalAge/count


    sc.stop()
  }

}
