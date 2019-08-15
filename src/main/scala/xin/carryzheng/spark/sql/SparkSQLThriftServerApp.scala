package xin.carryzheng.spark.sql

import java.sql.DriverManager

/**
  * $SPARK_HOME/sbin/start-thriftserver.sh
  *
  * @author zhengxin
  *         2019-08-07 17:03:15
  */
object SparkSQLThriftServerApp {

  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

//    val conn = DriverManager.getConnection("jdbc:hive2://10.0.31.239:10000", "hadoop", "")
    val conn = DriverManager.getConnection("jdbc:hive2://s.carryzheng.xin:10000", "hadoop", "")
//        val ps = conn.prepareStatement("select empno, ename, sal from emp")
//        val rs = ps.executeQuery()
//        while (rs.next()){
//          println("enpno: " + rs.getInt("empno") + ", ename: " + rs.getString("ename") + ", sal: " + rs.getDouble("sal"))
//        }

    //    val ps = conn.prepareStatement("select id, name from test_user2019 limit 20")
    //    val rs = ps.executeQuery()
    //    while (rs.next()){
    //      println("id: " + rs.getInt("id") + ", name: " + rs.getString("name"))
    //    }

    val ps = conn.prepareStatement("select id, name from tm limit 20")
    val rs = ps.executeQuery()
    while (rs.next()) {
      println("id: " + rs.getInt("id") + ", name: " + rs.getString("name"))
    }


    //    val ps = conn.prepareStatement("update tm set name = 'ccc' where id = 22")
    //    val rs = ps.executeQuery()


    rs.close()
    ps.close()
  }

}
