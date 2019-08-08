package xin.carryzheng.spark.datasource

import org.apache.spark.sql.SparkSession

/**
  * 操作mysql数据
  * http://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
  *
  * @author zhengxin
  *         2019-08-08 19:40:32
  */
object MySQLApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MySQLApp").master("local[2]").getOrCreate()

    // Loading data from a JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()

    jdbcDF.printSchema()
    jdbcDF.show()

//    // Saving data to a JDBC source
//    jdbcDF.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql:dbserver")
//      .option("dbtable", "schema.tablename")
//      .option("user", "username")
//      .option("password", "password")
//      .save()

    spark.stop()

  }

  /**
    * spark sql
    * */
//  CREATE TEMPORARY VIEW jdbcTable
//    USING org.apache.spark.sql.jdbc
//  OPTIONS (
//    url "jdbc:postgresql:dbserver",
//  dbtable "schema.tablename",
//  user 'username',
//  password 'password'
//  )
//
//  INSERT INTO TABLE jdbcTable
//    SELECT * FROM resultTable

}
