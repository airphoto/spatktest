package com.lhs.scala.jdbc

/**
 * <P>@ObjectName : ConnectionPoolTest</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/1/17 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

import java.sql.ResultSet
import org.slf4j.LoggerFactory

object ConnectionPoolTest extends App {

  val logger = LoggerFactory.getLogger(this.getClass)
  val query = "select * from dim_date"
  ConnectionPool.getConnection match {
    case Some(connection) =>
      val statement = connection.prepareStatement(query)
      ConnectionPool.withConnectOption(connection,statement) {(connection,statement)=>
        val result = statement.executeQuery(query)
        val my = new MyResultSet(result)
        val scalaResultSet = new MyResultSet(result) map { r => (r.getInt(1), r.getInt(2), r.getInt(3))}
        scalaResultSet take (10) foreach println _
      }
    case None =>
      println("Not geting connection from connection pooling")
  }
}

class MyResultSet(rs: ResultSet) extends Iterator[ResultSet] {
  override def hasNext: Boolean = rs.next()
  override def next(): ResultSet = rs
}
