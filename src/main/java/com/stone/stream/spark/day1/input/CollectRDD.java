package com.stone.stream.spark.day1.input;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
/**
*
*@className: CollectRDD
*@author:   kuiqwang
*@date:
*@desc: 集合输入成RDD
**/
@Slf4j
public class CollectRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("collectRDD");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD collectRDD = jsc.parallelize(Arrays.asList(new String[]{"one", "two", "three"}));
         long res = collectRDD.count();

        log.info("collect rdd res is [{}]", res);
    }
}
