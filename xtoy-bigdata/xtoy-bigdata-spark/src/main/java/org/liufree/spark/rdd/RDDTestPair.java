package org.liufree.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * @author lwx
 * 9/18/19
 * liufreeo@gmail.com
 */
public class RDDTestPair {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDDTestPair");
        conf.setMaster("local");
        //conf.setMaster("yarn"); 集群模式下才能与运行，通过spark-submit
        // conf.setMaster("spark://master:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //从hdfs中读取文件
        JavaRDD<String> lines = sc.textFile("hdfs://master:9000/xtoy/demo.txt");

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        System.out.println(counts.collect());

    }
}
