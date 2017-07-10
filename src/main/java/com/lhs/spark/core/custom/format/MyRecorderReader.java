package com.lhs.spark.core.custom.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by Administrator on 2017/7/6.
 */
public class MyRecorderReader extends RecordReader<Text, Text> {
    private LineReader lr;
    private Text key = new Text();
    private Text value = new Text();
    private long start;
    private long end;
    private long currentPos;
    private Text line = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) split;
        Configuration conf = context.getConfiguration();
        //获取分片文件对应的完整文件
        Path path = inputSplit.getPath();
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream is = fs.open(path);
        lr = new LineReader(is,conf);
        //获取分片文件的起始位置
        start = inputSplit.getStart();
        end = start + inputSplit.getLength();
        is.seek(start);
        if(start != 0){
            start += lr.readLine(new Text(),0,(int)Math.min(Integer.MAX_VALUE,end-start));
        }

        currentPos = start;
    }

    //针对每行数据进行处理
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(currentPos > end){
            return false;
        }
        currentPos += lr.readLine(line);
        System.out.println("currentPos-->"+currentPos);

        if(line.getLength() == 0){
            return false;
        }
        //如实需要被忽略的行，直接读下一行
        if(line.toString().startsWith("ignore")){
            currentPos += lr.readLine(line);
        }
        String[] words = line.toString().split(",");
        if(words.length < 2){
            System.err.print("line:"+line.toString()+".");
            return false;
        }
        key.set(words[0]);
        value.set(words[1]);
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(start == end){
            return 0.0f;
        }else{
            return Math.min(1.0f,(currentPos - start)/(float)(end-start));
        }
    }

    @Override
    public void close() throws IOException {

    }
}
