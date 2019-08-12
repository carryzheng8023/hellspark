package xin.carryzheng.spark.logdemo

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Mysql操作工具类
  *
  * @author zhengxin
  *         2019-08-12 17:34:10
  */
object MySqlUtils {

  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://hadopo01:3306/test?user=root&password=root")
  }

  def release(connection: Connection, ps: PreparedStatement): Unit = {
    try {
      if (null != ps)
        ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != connection)
        connection.close()
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}
