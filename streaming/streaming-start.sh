

val_kafka=10.81.248.102:9092,10.29.254.50:9092,10.81.248.203:9092
val_redis_host="10.81.248.203"
val_redis_port=6379
val_redis_password="XLhy!321YH"
val_redis_db=9


spark-submit \
--master yarn \
--deploy-mode cluster \
--num-executors 2 \
--executor-memory 2g \
--executor-cores 3 \
--driver-memory 2g \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.default.parallelism=40 \
--conf spark.sql.shuffle.partitions=40 \
--conf spark.streaming.backpressure.enabled=true \
--conf spark.streaming.kafka.maxRatePerPartition=6000 \
--conf spark.streaming.stopGracefullyOnShutdown=true \
--conf spark.yarn.maxAppAttempts=2 \
--conf spark.yarn.am.attemptFailuresValidityInterval=30m \
--name check-data \
--class CheckData \
streaming-1.0-SNAPSHOT.jar ${val_kafka} ${val_redis_host} ${val_redis_port} ${val_redis_password} ${val_redis_db}
