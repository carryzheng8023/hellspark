package xin.carryzheng.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author zhengxin
  *         2019-08-07 15:22:35
  */
object SQLContextApp {

  def main(args: Array[String]): Unit = {

    val path = args(0)

    //1. 创建相应context
    val sparkConf = new SparkConf()
//    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2. 相应处理
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //3.关闭资源
    sc.stop()


  }

}
