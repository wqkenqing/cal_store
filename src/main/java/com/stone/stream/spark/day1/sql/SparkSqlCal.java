package com.stone.stream.spark.day1.sql;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/7/22
 * @desc use spark sql
 */
@Slf4j
public class SparkSqlCal {
    public static void createDataFrame(SparkSession session) {
        Dataset<Row> df = session.read().json("/Users/wqkenqing/Desktop/tmp/people.json");
        long count = df.select("name").count();
        df.groupBy("age").count().show();
    }

    public static void createSql(SparkSession session) {
        Dataset<Row> df = session.read().json("/Users/wqkenqing/Desktop/tmp/people.json");
        df.createOrReplaceTempView("people");
        session.sql("select * from people").show();

    }
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .appName("sql session")
                .master("local[2]")
                .config("spark.sql.warehouse.dir", "/Users/wqkenqing/Desktop/deploy_code/cal_store/warehouse")
                .getOrCreate();
//        createDataFrame(session);
        createSql(session);
    }

}
