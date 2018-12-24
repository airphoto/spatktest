import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.{Jedis, JedisPool, Pipeline}

class RedisWriter private (host:String,port:Int,password:String,db:Int) extends ForeachWriter[Row] with Serializable{
  var jedis:Jedis = _
  var pipeline:Pipeline = _
  override def open(partitionId: Long, version: Long): Boolean = {
//    RedisWriter.makePool("10.81.248.203", 6379,3000,30,30,8,"XLhy!321YH",9)
    RedisWriter.makePool(host, port,3000,30,30,8,password,db)
    val pool = RedisWriter.getPool
    jedis = pool.getResource
    pipeline = jedis.pipelined()
    true
  }

  override def process(value: Row): Unit = {
    val types = value.getAs[String]("type")
    val targetDay = value.getAs[String]("target_day")
    val current = System.currentTimeMillis()
    val count = value.getAs[Long]("count")
    pipeline.hset(s"type_count_$targetDay",types,s"$current:$count")
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (pipeline != null ) {
      try {
        pipeline.sync()
        pipeline.close()
      }catch {
        case e:Exception=>println("pipeline 关闭或者同步数据异常")
      }
    }
    if (jedis != null) {
      try{
        jedis.close()
      }catch {
        case e:Exception=>println("jedis 关闭异常")
      }
    }
  }
}

object RedisWriter extends Serializable{
  def apply(host:String,port:Int,password:String,db:Int): RedisWriter = new RedisWriter(host,port,password,db)

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
