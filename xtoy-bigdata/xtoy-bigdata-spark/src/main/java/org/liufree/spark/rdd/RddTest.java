package org.liufree.spark.rdd;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.*;

public class RddTest {

    public static Pattern pattern = Pattern.compile(" ");

    public static void main(String[] args) {
        String logFile = "hdfs://host:9000/xtoy/jmeter.log"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().master("local").appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }

    public void test() {
        SparkSession spark = SparkSession.builder()
                .appName("ss").master("local").getOrCreate();
        JavaRDD<String> str = spark.read().textFile("").javaRDD();
        String s = new String("ss");
        String ss[] = s.split("");


        JavaRDD<String> sss = str.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] list = s.split("");
                List lists = Arrays.asList(list);
                return lists.iterator();
            }
        });
    }
}