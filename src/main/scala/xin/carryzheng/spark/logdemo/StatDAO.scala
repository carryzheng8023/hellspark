package xin.carryzheng.spark.logdemo

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  *
  * @author zhengxin
  *         2019-08-12 17:56:25
  */
object StatDAO {


  def insertDayVideoTrafficsAccessTopN(list: ListBuffer[DayVideoTrafficsStat]) = {
    var connection: Connection = null
    var ps: PreparedStatement = null

    try {

      connection = MySqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into day_video_traffics_topn_stat(day, cms_id, traffics) values (?,?,?)"

      ps = connection.prepareStatement(sql)

      for (ele <- list) {
        ps.setString(1, ele.day)
        ps.setLong(2, ele.cmsId)
        ps.setLong(3, ele.traffics)

        ps.addBatch()
      }

      ps.executeBatch()
      connection.commit()


    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, ps)
    }

  }


  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]) = {
    var connection: Connection = null
    var ps: PreparedStatement = null

    try {

      connection = MySqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into day_video_access_topn_stat(day, cms_id, times) values (?,?,?)"

      ps = connection.prepareStatement(sql)

      for (ele <- list) {
        ps.setString(1, ele.day)
        ps.setLong(2, ele.cmsId)
        ps.setLong(3, ele.times)

        ps.addBatch()
      }

      ps.executeBatch()
      connection.commit()


    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, ps)
    }

  }


  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]) = {
    var connection: Connection = null
    var ps: PreparedStatement = null

    try {

      connection = MySqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into day_video_city_topn_stat(day, cms_id, city, times, times_rank) values (?,?,?,?,?)"

      ps = connection.prepareStatement(sql)

      for (ele <- list) {
        ps.setString(1, ele.day)
        ps.setLong(2, ele.cmsId)
        ps.setString(3, ele.city)
        ps.setLong(4, ele.times)
        ps.setInt(5, ele.times_rank)

        ps.addBatch()
      }

      ps.executeBatch()
      connection.commit()


    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtils.release(connection, ps)
    }

  }

}
