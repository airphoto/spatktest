package com.lhs.spark.core.custom.format;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Administrator on 2016/6/22.
 */
public class FindMaxValueInputSplit extends InputSplit implements Writable,Serializable{

    private int m_startIndex;
    private int m_EndIndex;
    private ArrayWritable m_floatArray = new ArrayWritable(FloatWritable.class);

    public FindMaxValueInputSplit(){}

    /**
     * 这个自定义分类主要记录了Map函数的开始索引和结束索引，第一个map处理前50个小数，第二个map处理后50个
     * @param start 开始的位置
     * @param end   结束的位置
     */
    public FindMaxValueInputSplit(int start,int end){
        m_startIndex = start;
        m_EndIndex = end;
        int len = m_EndIndex - m_startIndex + 1;
        int index = m_startIndex;
        FloatWritable[] result = new FloatWritable[len];
        for (int i=0; i<result.length; i++){
            float f = FindMaxValueInputFormat.floatValues[index];
            FloatWritable fw = new FloatWritable();
            fw.set(f);
            result[i] = fw;
            index ++;
        }
        m_floatArray.set(result);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return (this.m_EndIndex-this.m_startIndex + 1);
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{"task-1","task-2"};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.m_startIndex);
        out.writeInt(this.m_EndIndex);
        this.m_floatArray.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.m_startIndex = in.readInt();
        this.m_EndIndex = in.readInt();
        this.m_floatArray.readFields(in);
    }

    public int getM_EndIndex() {
        return m_EndIndex;
    }

    public int getM_startIndex() {
        return m_startIndex;
    }

    public ArrayWritable getM_floatArray() {
        return m_floatArray;
    }
}
