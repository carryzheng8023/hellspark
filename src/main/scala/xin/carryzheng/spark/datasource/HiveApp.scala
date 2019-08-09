package xin.carryzheng.spark.datasource

import org.apache.spark.sql.SparkSession

/**
  * 处理hive数据
  * http://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
  * @author zhengxin
  *         2019-08-08 17:43:59
  */
object HiveApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveApp").master("local[2]").getOrCreate()

    spark.table("emp").show

    spark.sql("select deptno, count(1) from emp group by deptno").show

    // 设置分区数量，默认200
//    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "10")

//    spark.sql("select deptno, count(1) as mount from emp group by deptno").write.saveAsTable("dept_number")

    spark.stop()
  }

}
