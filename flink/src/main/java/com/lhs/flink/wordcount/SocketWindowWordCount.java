package com.lhs.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception{
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
        }catch (Exception e){
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        // 返回一个可用的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost",port,"\n");

        DataStream<WordWithCount> windowCounts = text
                .flatMap(new LineSplit())
                .keyBy("word")
                .timeWindow(Time.seconds(5),Time.seconds(1))
                .reduce(new WordCountReduce());

        windowCounts.print().setParallelism(1);
        // 下面真正开始执行任务，上边的flatMap，reduce方法只是告诉整个程序，我们采用了什么样的算子
        // 1：生成StreamGraph，代表程序的拓扑结构，从用户代码直接生成的图
        // 2: 生成JobGraph，这个图要交给flink去生成task的图
        // 3：生成一系列的配置
        // 4：将JobGraph和配置交给flink集群去运行。如果不是本地运行的话，还会把jar文件通过网络发给其他节点。
        // 5：以本地模式运行的话，可以看到启动过程，如启动性能度量、web模块、JobManager、ResourceManager、taskManager等等
        // 6：启动任务。值得一提的是在启动任务之前，先启动了一个用户类加载器，这个类加载器可以用来做一些在运行时动态加载类的工作。
        env.execute("wocket window wordCount");
    }

    public static final class WordCountReduce implements ReduceFunction<WordWithCount>{

        public WordWithCount reduce(WordWithCount wordWithCount, WordWithCount t1) throws Exception {
            return new WordWithCount(wordWithCount.word,t1.count+wordWithCount.count);
        }
    }

    public static final class LineSplit implements FlatMapFunction<String,WordWithCount>{

        public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
            for (String word:s.split("\\s")){
                if (word.length() > 0){
                    collector.collect(new WordWithCount(word,1));
                }
            }
        }
    }

    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){};
        public WordWithCount(String word,long count){
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word +" : "+count;
        }
    }
}
