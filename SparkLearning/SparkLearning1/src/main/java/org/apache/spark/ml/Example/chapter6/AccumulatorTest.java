package org.apache.spark.ml.Example.chapter6;

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

/**
 * 第六章： spark编程进阶
      6.2  累加器
 * @author yudan
 *
 */
public class AccumulatorTest {
    public static void main(String[] args) {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        test1();
    }
    
    public static void test1(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd5").setMaster("local[3]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> rdd = sc.textFile("file://e/tmp/test5/");
        rdd.persist(StorageLevel.MEMORY_ONLY());
        ///System.out.println(rdd.collect().size());
        
        final Accumulator<Integer> allLines = sc.accumulator(0, "total");    //总行数
        final Accumulator<Integer> rootLines = sc.accumulator(0, "root");    //包含root的行数
        
//        rdd.map(new Function<String, String>() {
//            @Override
//            public String call(String line) throws Exception {
//                allLines.add(1);
//                System.out.println("local:" + blankLines.localValue());
//                return line;
//            }
//        }).count();  
        
        //注意只有执行了行为操作才会执行map
        rdd.flatMap(new FlatMapFunction<String, String>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> call(String line) throws Exception {
                if(line.contains("root")){
                    rootLines.add(1);
                    System.out.println("local root:" + rootLines.localValue());
                }
                allLines.add(1);
                return  Arrays.asList(line.split(" "));
            }
        }).count();
        
        System.out.println("====total line all :" + allLines.value());
        System.out.println("====root line all :" + rootLines.value());
    }
    
}



