package com.lhs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;
import java.util.Properties;

/**
 * Created by Administrator on 2017/7/14.
 */
public class DataFrameCreate {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("java");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        Properties properties = new Properties();
        properties.put("user","root");
        properties.put("password","123456");

        Dataset<Row> dfr = sqlContext.read().jdbc("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf-8","dim_date",properties);
        dfr.show();
        jsc.stop();
    }
}
