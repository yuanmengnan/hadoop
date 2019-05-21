package org.apache.spark.ml.Example.chapter5.inputoutput;

import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 第五章： 输入与输出
      5.2.5 Object文件 
      
          对象文件看起来就像是对 SequenceFile 的简单封装，它允许存储只包含值的 RDD。
          和 SequenceFile 不一样的是，对象文件是使用 Java 序列化写出来的。
 * @author yudan
 *
 */
public class ObjectFile {
    
    public static void main(String[] args) throws IOException {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        write();
        
        read();
    }
    
    public static void write(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CsvFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String,Integer> data =sc
                .parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("zhouyang", 1),
                        new Tuple2<String, Integer>("jack", 2),
                        new Tuple2<String, Integer>("bob", 3)));
        data.saveAsObjectFile("file:///E:/e/tmp/test5/obj/out/1.obj");
        
        sc.close();
    }
    
    public static void read(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CsvFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<Tuple2<String,Integer>> input= sc.objectFile("file:///E:/e/tmp/test5/obj/out/1.obj");
        for(Tuple2<String,Integer> tuple:input.collect()){
            System.out.println(tuple._1()+" -> "+tuple._2());
        }
        
        sc.close();
    }
}
