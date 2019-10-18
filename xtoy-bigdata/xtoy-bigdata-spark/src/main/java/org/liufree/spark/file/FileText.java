package org.liufree.spark.file;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author lwx
 * 9/19/19
 * liufreeo@gmail.com
 */
public class FileText {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RddTest1");
        conf.setMaster("local");
        //conf.setMaster("yarn"); 集群模式下才能与运行，通过spark-submit
        // conf.setMaster("spark://master:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //从hdfs中读取文件
        JavaRDD<String> logData = sc.textFile("hdfs://master:9000/xtoy/people.json");

        class ParseJson implements FlatMapFunction<Iterator<String>, People> {
            @Override
            public Iterator<People> call(Iterator<String> lines) throws Exception {
                ArrayList<People> people = new ArrayList<People>();
                ObjectMapper mapper = new ObjectMapper();
                while (lines.hasNext()) {
                    String line = lines.next();
                    try {
                        people.add(mapper.readValue(line, People.class));
                    } catch (Exception e) {
// 跳过失败的数据
                    }
                }
                return people.iterator();
            }
        }
        JavaRDD<String> input = sc.textFile("hdfs://master:9000/xtoy/people.json");
        JavaRDD<People> result = input.mapPartitions(new ParseJson());

        System.out.println(JSON.toJSON(result.collect()));

        class WriteJson implements FlatMapFunction<Iterator<People>, String> {
            @Override
            public Iterator<String> call(Iterator<People> people) throws Exception {
                ArrayList<String> text = new ArrayList<String>();
                ObjectMapper mapper = new ObjectMapper();
                while (people.hasNext()) {
                    People person = people.next();
                    text.add(mapper.writeValueAsString(person));
                }
                return text.iterator();
            }
        }

        JavaRDD<People> result1 = input.mapPartitions(new ParseJson());
        JavaRDD<String> formatted = result1.mapPartitions(new WriteJson());
        formatted.saveAsTextFile("hdfs://master:9000/xtoy/peopleRes.json");
    }
}

