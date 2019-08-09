package xin.carryzheng.spark.sql

import java.sql.DriverManager

/**
  *
  * @author zhengxin
  *         2019-08-07 17:03:15
  */
object SparkSQLThriftServerApp {

  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://hadoop01:10000", "zx", "")
    val ps = conn.prepareStatement("select empno, ename, sal from emp")
    val rs = ps.executeQuery()
    while (rs.next()){
      println("enpno: " + rs.getInt("empno") + ", ename: " + rs.getString("ename") + ", sal: " + rs.getDouble("sal"))
    }

    rs.close()
    ps.close()
  }

}
