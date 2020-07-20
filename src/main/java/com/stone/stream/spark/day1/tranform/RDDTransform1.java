package com.stone.stream.spark.day1.tranform;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/7/14
 * @desc Value数据类型的Transformation算子
 */
@Slf4j
public class RDDTransform1 {
    static JavaSparkContext jsc;
    static {
        SparkConf conf = new SparkConf().setAppName("val's RDD");
        jsc = new JavaSparkContext(conf);
    }
    //map 对val进行one to one 操作.
    public void map(JavaRDD<String> rdd) {
        //map
        rdd.map(s -> {
            return s.replace("tst", "");
        }).foreach(s -> System.out.println(s));
    }


    //flatMap 对val进行one to many 操作 . lambda中返回的是迭代器.
    public static void flatMap(JavaRDD<String> rdd) {
        JavaRDD<String> ndd = rdd.flatMap(s -> {
            return Arrays.asList(s.split("\\s")).iterator();
        });

        ndd.foreach(n -> {
            System.out.println(n);
        });
    }


    //glom是将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
    public static void glom(JavaRDD<String> rdd) {
        JavaRDD<List<String>> ndd = rdd.glom();
        ndd.foreach(n -> {
            System.out.println("长度:");
            System.out.println(n.size());
        });
    }

    public static void mapPartitions(JavaRDD<String> rdd) {
        List<Iterator<String>> ndd = rdd.mapPartitions(s1 -> {
                    s1.forEachRemaining(s -> {
                        s.replace("tst", "");
                    });
                    return Arrays.asList(s1).iterator();
                }
        ).collect();
        System.out.println(ndd);
    }
    //filter 算子
    public static void filter(JavaSparkContext jsc) {
        List<String> nlist = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            nlist.add(i + "");
        }

        jsc.parallelize(nlist).filter(s -> {
            return Integer.valueOf(s) > 2;
        }).foreach(s -> {
            System.out.println(s);
        });
    }

    public static void distinct(JavaSparkContext jsc) {
        List<String> nlist = new ArrayList<>();
        nlist.add("ss");
        nlist.add("ss");
        nlist.add("ss");
        nlist.add("ss");
        nlist.add("sss");
        jsc.parallelize(nlist).distinct().foreach(s->{
            System.out.println(s);
        });
    }

    public static void union() {
        List<String> nlist = new ArrayList<>();
        List<String> nlist1 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            nlist.add(i + "");
        }
        for (int i = 10; i >0 ; i--) {
            nlist1.add(i + "");
        }
//        JavaRDD<String> aRDD = jsc.parallelize(nlist).union(jsc.parallelize(nlist1));
//        aRDD.foreach(s->{
//            System.out.println(s);
//
//        });
//        JavaRDD<String> nRDD = jsc.parallelize(nlist).intersection(jsc.parallelize(nlist1));
//        nRDD.foreach(s->{
//            System.out.println(s);
//        });
//        JavaRDD<String> nRDD = jsc.parallelize(nlist).subtract(jsc.parallelize(nlist1));
        JavaPairRDD<String, String> nRDD = jsc.parallelize(nlist).cartesian(jsc.parallelize(nlist1));

        nRDD.foreach(s->{
            System.out.print(s._1);
            System.out.print(" ");
            System.out.println(s._2);
        });

    }




    public static void main(String[] args) {
        union();

    }
}
