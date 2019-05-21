package org.apache.spark.ml.Example.chapter5.inputoutput;

import java.util.Map.Entry;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 第五章： 输入与输出
      5.2.1　文本文件 
 * @author yudan
 *
 */
public class TextFile {
    public static void main(String[] args) {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        test1();
    }
    
    public static void test1(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd5").setMaster("local[1]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> rdd = sc.textFile("file://e/tmp/test5/");
//        JavaRDD<String> rdd = sc.textFile("hdfs://hadoop-1:8020/data/yearhot/data3.log");
//        for (String line : rdd.collect()) {
//            System.out.println(line);
//        }
        
        rdd.saveAsTextFile("file:///e/tmp/test5/out");
        //rdd.saveAsTextFile("hdfs://hadoop-1:8020/data/yearhot/data3.rdd");
        
        //rdd.saveAsTextFile("file:///e/tmp/test5/outzip", GzipCodec.class);
        sc.close();
    }
    
    public static void test2(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd5").setMaster("local[1]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
       
        JavaPairRDD<String, String> pairRdd = sc.wholeTextFiles("file:///e/tmp/test5/out");
        for (Entry<String, String> e: pairRdd.collectAsMap().entrySet()) {
            System.out.println(e.getKey() + "--" + e.getValue());
        }
        
        pairRdd.saveAsTextFile("file:///e/tmp/test5/out2");
        
        sc.close();
    }
}



