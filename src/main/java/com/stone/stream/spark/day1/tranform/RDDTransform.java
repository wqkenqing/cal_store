package com.stone.stream.spark.day1.tranform;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTransform {

    public static void map(JavaRDD<String> rdd) {

        JavaRDD<String> rddN = rdd.map(row -> {
            return row + "\t" + "yes";
        });

        rddN.foreach(res -> {
            System.out.println(res);
        });

    }

    public static void filter(JavaRDD<String> rdd) {
        long res = rdd.count();
        JavaRDD rdd2 = rdd.filter(row -> {
            String[] reses = row.split("\t");
            String res1 = reses[6];
            try {

                if (StringUtils.isBlank(res1)) {
                    return false;
                } else if (Integer.valueOf(res1) > 100) {
                    return true;
                }
            } catch (Exception e) {
                System.out.println("wrong");
            }
            return false;
        });

        long res1 = rdd2.count();
        System.out.println("结果1:" + res);
        System.out.println("结果2:" + res1);
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("transform");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = jsc.textFile("/Users/kuiq.wang/Desktop/upload/yd_conver.txt");
//        map(rdd);
        filter(rdd);
    }
}
