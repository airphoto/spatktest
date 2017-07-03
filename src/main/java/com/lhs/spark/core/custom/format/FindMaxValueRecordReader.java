package com.lhs.spark.core.custom.format;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.Serializable;

/**
 * 定义输入到map函数的输入格式
 * key为偏移量
 * value为float数组，长度为50
 */
public class FindMaxValueRecordReader extends RecordReader<IntWritable, ArrayWritable> implements Serializable{

    private int mEnd;
    private int mIndex;
    private int mStart;
    private IntWritable key = null;
    private ArrayWritable value = null;
    private FindMaxValueInputSplit fmvSplit = null;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fmvSplit = (FindMaxValueInputSplit)split;
        this.mStart = fmvSplit.getM_startIndex();
        this.mEnd = fmvSplit.getM_EndIndex();
        this.mIndex = this.mStart;
    }

    /**
     * 输出的key为intWritable
     * 输出的value为ArrayWritable
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(key == null){
            key = new IntWritable();
        }
        if(value == null){
            value = new ArrayWritable(FloatWritable.class);
        }

        if(mIndex < mEnd){
            key.set(mIndex);
            value = fmvSplit.getM_floatArray();
            mIndex = mEnd +1;
            return true;
        }
        return false;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public ArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(this.mIndex == this.mEnd){
            return 0.0f;
        }else {
            return Math.min(1.0f,(this.mIndex-this.mStart)/(float)(this.mEnd-this.mStart));
        }
    }

    @Override
    public void close() throws IOException {

    }
}
