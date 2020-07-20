package com.stone.stream.spark.day1.tranform;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/7/14
 * @desc key value 类型算子
 */
@Slf4j
public class RDDTransform2 {
    static JavaSparkContext jsc;

    static {
        SparkConf conf = new SparkConf().setAppName("val's RDD");
        jsc = new JavaSparkContext(conf);
    }

    public static void maptoPair() {
        List<String> nlist = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            nlist.add(i + "");
        }
        jsc.parallelize(nlist).mapToPair(s -> {
            return new Tuple2<>(s, 1);
        }).foreach(s -> {
            System.out.print(s._1);
            System.out.print(" ");
            System.out.println(s._2);
        });
    }

    public static void combineByKey() {
        List<String> nlist = new ArrayList<>();
        List<String> nlist1 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            nlist.add("key" + i);
        }
        JavaPairRDD<String, String> pRDD = jsc.parallelize(nlist).mapToPair(s -> {
            return new Tuple2<>(s, 1 + "");
        });
        JavaPairRDD<String, Integer> pRDD1 = jsc.parallelize(nlist).mapToPair(s -> {
            return new Tuple2<>(s, 2);
        });

        Function<String, Tuple2<String, String>> createCombiner = (s) -> {
            return new Tuple2<>(s, "sss");
        };
        Function2<Tuple2<String, String>, String, Tuple2<String, String>> mergeValue = (Tuple2<String, String> tp, String s1) -> {
            return new Tuple2<String, String>(tp._1, tp._2 + s1);
        };

        Function2<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>> mergeCombiner = (Tuple2<String, String> tp, Tuple2<String, String> tp1) -> {
            return new Tuple2<>(tp._1, tp._2 + tp1._2);
        };

        pRDD.combineByKey(createCombiner, mergeValue, mergeCombiner).foreach(s -> {
            System.out.print(s._1);
            System.out.print(" ");
            System.out.println(s._2);
        });
    }

    public static void reduceByKey() {
        List<String> nlist = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            nlist.add(i + "");
        }
        for (int i = 0; i < 10; i++) {
            nlist.add(i + "");
        }
        JavaPairRDD<String, Integer> nRDD = jsc.parallelize(nlist).mapToPair(s -> {
            return new Tuple2<>(s, 2);
        });
        Function2<Integer, Integer, Integer> rFunction = (Integer one, Integer two) -> {
            return one + two;
        };

        nRDD.reduceByKey(rFunction).foreach(s -> {
            System.out.print(s._1);
            System.out.print(" ");
            System.out.println(s._2);
        });


    }

    public static void foldByKey() {
        List<String> nlist = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            nlist.add(i + "");
        }
        for (int i = 0; i < 10; i++) {
            nlist.add(i + "");
        }

        Function2<Integer, Integer, Integer> foldFunction = (Integer t1, Integer t2) -> {
            return t1 * t2;
        };
        jsc.parallelize(nlist).mapToPair(s -> {
            return new Tuple2<>(s, 2);
        }).foldByKey(2, foldFunction).groupByKey().foreach(s->{
            System.out.print(s._1);
            System.out.print(" ");
            System.out.println(s._2);
        });


    }
    public static void main(String[] args) {
        foldByKey();
    }
}
