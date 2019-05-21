package org.apache.spark.ml.Example.chapter6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/**
 * 第六章： spark编程进阶
      6.3  广播 
            （第 8 章中会讨论如何使用 Kryo 这种更快的序列化）
 * @author yudan
 *
 */

public class BroadcastTest {

    public static void main(String[] args) {
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd5").setMaster("local[3]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        
        final Broadcast<String[]> signPrefixes = sc.broadcast(new String[]{"aaa", "bbb", "ccc"}); 
        
        JavaRDD<String> rdd = sc.textFile("file://e/tmp/test5/");
        
        rdd.map(new Function<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(String line) throws Exception {
                String[] bc = signPrefixes.value();
                System.out.println(Thread.currentThread().getName() + bc);
                return line;
            }
        }).count();
        
        sc.close();
    }
}
