package xin.carryzheng.spark.dataframe

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * 将RDD转为DataFrame
  * http://spark.apache.org/docs/latest/sql-getting-started.html#interoperating-with-rdds
  * @author zhengxin
  *         2019-08-08 14:57:54
  */
object RDD2DataFrameApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("RDD2DataFrameApp").master("local[2]").getOrCreate()

//    inferReflection(spark)
    programme(spark)

    spark.stop()
  }

  def programme(spark: SparkSession): Unit ={

    val rdd_people = spark.sparkContext.textFile("file:///Users/zhengxin/tmp/people.txt")

    val people_row_rdd = rdd_people.map(_.split(",")).map(line => Row(line(0), line(1).toInt))

    val structType = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))

    val df_people = spark.createDataFrame(people_row_rdd, structType)

    df_people.printSchema()
    df_people.show()

  }

  def inferReflection(spark: SparkSession): Unit ={
    val rdd_people = spark.sparkContext.textFile("file:///Users/zhengxin/tmp/people.txt")

    import spark.implicits._
    val df_people = rdd_people.map(_.split(",")).map(line => People(line(0), line(1).toInt)).toDF()

    df_people.show()
  }

  case class People(name: String, age: Int)

}
