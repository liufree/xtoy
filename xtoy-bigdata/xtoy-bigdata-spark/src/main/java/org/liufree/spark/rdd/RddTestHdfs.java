package org.liufree.spark.rdd;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author lwx
 * 9/17/19
 * liufreeo@gmail.com
 */
@Slf4j
public class RddTestHdfs {
    /**
     * 运行
     * /opt/bigdata/spark/bin/spark-submit \
     * --class org.liufree.spark.rdd.RddTest \
     * --master spark://host:7077 \
     * target/xtoy-bigdata-spark-1.0-SNAPSHOT.jar
     **/

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("RddTest1");
        conf.setMaster("local");
        //conf.setMaster("yarn"); 集群模式下才能与运行，通过spark-submit
        // conf.setMaster("spark://master:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //内存中生成data
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        //从hdfs中读取文件
        JavaRDD<String> logData = sc.textFile("hdfs://host:9000/xtoy/jmeter.log");

        //过滤 ,对日志中的错误计数，返回带error字段的每一行
        JavaRDD<String> data1 = logData.filter(s -> s.contains("error"));
        JavaRDD<String> data2 = logData.filter(s -> s.contains("1"));
        //联合
        JavaRDD<String> data3 = data1.union(data2);

        //写到hdfs中去
        // data3.saveAsTextFile("hdfs://master:9000/xtoy/liu1.txt");

        //大多数情况下，不能通过collect()收集到驱动器进程上，因为一般都很大
        //需要放在HDFS这种分布式系统上
        List<String> stringList = data3.collect();
        System.out.println(stringList);

        //计数
        long numData1 = data1.count();
        log.warn("诉讼诉讼" + numData1);

        //惰性求值
        //惰性求值意味着当我们对 RDD 调用转化操作(例如调用 map() )时,操作不会立即执行。
        //相反,Spark 会在内部记录下所要求执行的操作的相关信息

        //reduce,聚合
        JavaRDD<Integer> lineLengths = data2.map(s -> s.length());
        lineLengths.foreach(integer -> System.out.println("tellme" + integer));
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println(totalLength);

        //map,转换匹配
        JavaRDD<Integer> lineLengths1 = data2.map(s -> s.length());
        Integer totalLength1 = lineLengths1.reduce((a, b) -> a + b);
        System.out.println(totalLength1);


        //将function单独写为一个类
        class GetLength implements Function<String, Integer> {
            @Override
            public Integer call(String s) {
                return s.length();
            }
        }
        class Sum implements Function2<Integer, Integer, Integer> {
            @Override
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        }

        JavaRDD<String> lines11 = sc.textFile("hdfs://host:9000/xtoy/jmeter.log");
        JavaRDD<Integer> lineLengths11 = lines11.map(new GetLength());
        int totalLength11 = lineLengths11.reduce(new Sum());
        System.out.println(totalLength11);

        //flagmap,切分行为单个单词
        JavaRDD<String> stringJavaRDD1 = data2.flatMap((FlatMapFunction<String, String>) s -> {
            String[] ss = s.split(" ");
            List<String> list = Arrays.asList(ss);
            return list.iterator();
        });

        System.out.println(stringJavaRDD1.first());


        //采样，取一半
        JavaRDD<String> caiyangList = data2.sample(false, 0.5);

        JavaRDD<String> wordList = data2.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        //取前两位
        System.out.println(wordList.take(2));

        //每个单词的个数
        System.out.println(wordList.countByValue());

        //不同的RDD类型转换
        /**
         JavaDoubleRDD javaDoubleRDD = wordList.mapToDouble(new DoubleFunction<String>() {
        @Override public double call(String s) throws Exception {
        if (s.equals("liu")) {
        return 1;
        }
        return Double.valueOf(s);
        }
        });
         System.out.println(javaDoubleRDD.collect());
         **/
        //持久化
        data1.persist(StorageLevel.DISK_ONLY());
        sc.close();
    }
}
