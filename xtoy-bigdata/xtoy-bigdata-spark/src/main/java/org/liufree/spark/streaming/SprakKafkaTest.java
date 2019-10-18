package org.liufree.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author lwx
 * 9/20/19
 * liufreeo@gmail.com
 */
public class SprakKafkaTest {

    public static void main(String[] args) throws Exception{
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
// 以端口7777作为输入来源创建DStream
        JavaDStream<String> lines = jssc.socketTextStream("local", 9999);
// Split each line into words
        JavaDStream<String> errorLines = lines.filter(s -> s.contains("tellme"));
// 打印出有"error"的行
        errorLines.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}
