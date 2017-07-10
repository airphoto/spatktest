package com.lhs.scala.jdbc
/**
 * <P>@ObjectName : ConnectionPool</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/1/17 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

import com.jolbox.bonecp.BoneCP
import com.jolbox.bonecp.BoneCPConfig
import org.slf4j.LoggerFactory
import java.sql.{Statement, Connection}

object ConnectionPool {

  val logger = LoggerFactory.getLogger(this.getClass)

  private val connectionPool = {
    try {
      Class.forName("org.postgresql.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test")
      config.setUsername("root")
      config.setPassword("123456")
      config.setMinConnectionsPerPartition(2)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(3)
      config.setCloseConnectionWatch(true)// if connection is not closed throw exception
      config.setLogStatementsEnabled(true) // for debugging purpose
      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        logger.warn("Error in creation of connection pool"+exception.printStackTrace())
        None
    }
  }

  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }

  def withConnectOption(connection: Connection,statement: Statement)(op:(Connection,Statement)=>Unit): Unit ={
    try{
      op(connection,statement)
    }catch {
      case exception: Exception =>
        logger.warn("Error in excuting query" + exception.printStackTrace())
    }finally {
      if(statement!=null && !statement.isClosed) statement.close()
      if(connection!=null && !connection.isClosed) connection.close()
    }
  }
}
