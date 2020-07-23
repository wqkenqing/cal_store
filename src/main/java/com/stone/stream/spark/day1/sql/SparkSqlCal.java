package com.stone.stream.spark.day1.sql;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.Test;
import org.apache.spark.sql.functions.*;

import java.util.Arrays;


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
    /**
     * 员工测试
     * */
    public static void testEmployee(SparkSession session) {
        Dataset<Row> df1 = session.read().json("/Users/wqkenqing/Desktop/北风/department.json");
        Dataset<Row> df = session.read().json("/Users/wqkenqing/Desktop/北风/employee.json");
//        df.join(df1, df.col("id").equalTo(df1.col("depId"))).agg(df.col("age"), df1.col("age")).show();
        df.filter("age>21").join(df1, df1.col("id").equalTo(df.col("depId"))).groupBy(df.col("name")).agg(functions.max(df.col("age"))).show();
    }
    /**
     *
     *
     * */
    public static void testAction(SparkSession session) {
        Dataset<Row> df = session.read().json("/Users/wqkenqing/Desktop/北风/employee.json");
//        df.foreach();
        Arrays.asList(df.collect()).forEach(s->{
            System.out.println(s);
        });
    }
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .appName("sql session")
                .master("local[2]")
                .config("spark.sql.warehouse.dir", "/Users/wqkenqing/Desktop/deploy_code/cal_store/warehouse")
                .getOrCreate();
//        createDataFrame(session);
//        createSql(session);
        testEmployee(session);
    }

}
