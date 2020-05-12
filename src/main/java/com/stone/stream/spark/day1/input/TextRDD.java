package com.stone.stream.spark.day1.input;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.JavaEsRDD;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/5/11
 * @desc text 输入源
 */
@Slf4j
public class TextRDD {
    /**
     * @desc: read json
     * @date:
     **/
    public static void readJson(JavaSparkContext jsc, String path) {
        JavaRDD<String> tRDD = jsc.textFile(path, 3);

        tRDD.foreach(row -> {
            JSONObject rowObj = (JSONObject) JSONObject.parse(row);
            rowObj.entrySet().forEach(entry -> {
                System.out.println(entry.getKey());
                System.out.println(entry.getValue());
            });
        });

    }

    /**
     * @desc:
     * @date:
     **/
    public static void readFile() {
        //同readJSON
    }

    public void readHdfs() {
        //同readJSON
    }

    public static void readES(String url, String index) {
        SparkConf conf = new SparkConf().setAppName("es_count").set("es.nodes", "data1:9200");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "funnylog_test");
        long es = esRDD.count();
        System.out.println("res is :" + es);

    }

    public static void main(String[] args) {
        readES("", "");
    }
}
