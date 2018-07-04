package com.lhs.spark.structstreaming

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.{Jedis, JedisPool}

class RedisWriterTest extends ForeachWriter[Row] with Serializable{
  var jedis:Jedis = _
  override def open(partitionId: Long, version: Long): Boolean = {
    RedisWriterTest.makePool("192.168.10.48", 6379,3000,30,30,8,"ljPfe1xEPbtP",9)
    val pool = RedisWriterTest.getPool
    jedis = pool.getResource
    println(pool,jedis,partitionId,version)
    true
  }

  override def process(value: Row): Unit = {
    val key = value.getAs[String]("value")
    val v = value.getAs[Long]("count")
    println(key,v)
    jedis.set(s"struct:$key",s"$v")
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (jedis != null) jedis.close()
    println("close-->"+jedis)
  }
}

object RedisWriterTest extends Serializable{
  def apply(): RedisWriterTest = new RedisWriterTest()

  @transient private var pool: JedisPool = null

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int ,passwd:String,database:Int): Unit = {
    makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, true, 10000,passwd,database)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
               testOnReturn: Boolean, maxWaitMillis: Long,passwd:String,database:Int): Unit = {
    if(pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout,passwd,database)

      val hook = new Thread{
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool: JedisPool = {
    pool
  }

}
