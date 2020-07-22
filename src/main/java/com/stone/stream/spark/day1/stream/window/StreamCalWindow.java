package com.stone.stream.spark.day1.stream.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/7/16
 * @desc stream算子
 */
public class StreamCalWindow {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("NetWorkStream");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("116.196.81.123", 9989);
        jsc.checkpoint("/Users/wqkenqing/Desktop/tmp/spark");

        Function2<Integer, Integer, Integer> rFunction = (Integer one, Integer two) -> {
            return one + two;
        };

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> uFunction = (List<Integer> tl, Optional<Integer>op) -> {
            Integer state = 0;
            if (op.isPresent()) {
                 state= op.get();
            }
            for (Integer t : tl) {
                state = state + t;
            }
            return Optional.of(state);
        };

        //streaming operate
        JavaPairDStream<String, Integer> pairDStream = lines.mapToPair(s -> {
            return new Tuple2<>(s, 1);
        });
        pairDStream.updateStateByKey(uFunction).print();

        jsc.start();
        jsc.awaitTermination();
    }

}
