package org.apache.spark.ml.Example.chapter5.inputoutput;

import java.util.List;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 第五章： 输入与输出
      5.2　输出压缩文件 
      
       Spark 的 textFile() 方法可以处理压缩过的输入，但即使输入数据 被以可分割读取的方式压缩，Spark 也不会打开 splittable。
             因此，如果 你要读取单个压缩过的输入，最好不要考虑使用 Spark 的封装，
             而是使用 newAPIHadoopFile 或者 hadoopFile，并指定正确的压缩编解码器
 * @author yudan
 *
 */
public class ZipFile {
    public static void main(String[] args) {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        read();
    }
    
    public static void write(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd5").setMaster("local[1]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> rdd = sc.textFile("file://e/tmp/test5/"); 
        rdd.saveAsTextFile("file:///e/tmp/test5/outzip", GzipCodec.class);
        sc.close();
    }
    
    public static void read(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd5").setMaster("local[1]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> rdd = sc.textFile("file:///e/tmp/test5/outzip"); 
        List<String> list = rdd.collect();
        System.out.println(list);
        sc.close();
    }
}



