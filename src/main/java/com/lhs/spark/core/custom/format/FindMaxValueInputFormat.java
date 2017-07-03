package com.lhs.spark.core.custom.format;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 返回一个inputSplit集合
 * 这个例子一共返回两个InputSplit，两个Map
 * 随机产生100个0-1的float数组
 */
public class FindMaxValueInputFormat extends InputFormat<IntWritable,ArrayWritable> implements Serializable{
    public static float[] floatValues;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        int numOfValues = context.getConfiguration().getInt("num_of_values",100);
        floatValues = new float[numOfValues];
        Random random = new Random();

        for (int i=0;i<numOfValues;i++){
            floatValues[i] = random.nextFloat();
        }

        int numSplits = context.getConfiguration().getInt("mapred.map.tasks",2);
        int beg = 0;
        int length = (int)Math.floor(numOfValues/numSplits);
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        int end = length - 1;

        for (int i=0;i<numSplits -1;i++){
            FindMaxValueInputSplit split = new FindMaxValueInputSplit(beg,end);
            splits.add(split);

            beg = end+1;
            end = end + length -1;
        }

        FindMaxValueInputSplit split = new FindMaxValueInputSplit(beg,numOfValues -1);
        splits.add(split);
        return splits;
    }

    @Override
    public RecordReader<IntWritable, ArrayWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new FindMaxValueRecordReader();
    }
}
