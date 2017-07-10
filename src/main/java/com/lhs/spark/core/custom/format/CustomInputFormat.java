package com.lhs.spark.core.custom.format;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by Administrator on 2017/7/5.
 */

public class CustomInputFormat extends FileInputFormat<Text,Text>{
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MyRecorderReader();
    }
}
