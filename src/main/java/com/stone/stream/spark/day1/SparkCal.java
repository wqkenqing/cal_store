package com.stone.stream.spark.day1;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.glassfish.hk2.api.Self;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/5/11
 * @desc
*/
@Slf4j
public class SparkCal {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("text_count");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> tRDD = sc.textFile("/Users/kuiq.wang/Desktop/upload/yd_conver.txlog.t", 3);

        long res = tRDD.count();

        log.info("text_count's result is [{}]", res);

    }
}
