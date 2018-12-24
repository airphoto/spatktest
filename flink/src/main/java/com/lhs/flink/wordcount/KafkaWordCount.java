package com.lhs.flink.wordcount;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class KafkaWordCount {
    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<String>("test",new SimpleStringSchema(),properties);
        consumer010.setStartFromEarliest();

        DataStream<String> stream = env.addSource(consumer010);

        stream.print().setParallelism(1);
        env.execute("exe from kafka");

    }
}
