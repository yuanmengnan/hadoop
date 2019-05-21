package org.apache.spark.ml.Example.chapter6;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * 第六章： spark编程进阶
      6.4　基于分区进行操
           分区中共享连接池等，只执行一次的操作
      
      基于分区的方式使得创建连接的次数不会那么多，然后每个分区的数据也可以平均分到每个节点的executor上，避免了内存不足产生的异常，
      当然前提是要合理的分配分区数，既不能让分区数太多，也不能让每个分区的数据太多，
      还有要注意数据倾斜的问题，因为当数据倾斜造成某个分区数据量太大同样造成OOM（内存溢出）。

 * @author yudan
 *
 */
public class PartitionTest {
    
    public static void main(String[] args) {
        test2();
    }
    
    public static void test1(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd5").setMaster("local[3]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        final Broadcast<String[]> signPrefixes = sc.broadcast(new String[]{"aaa", "bbb", "ccc"}); 
        
        JavaRDD<String> rdd = sc.textFile("file://e/tmp/test5/");
        
        List<String> result = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> call(Iterator<String> it) throws Exception {
                getJdbcConn(signPrefixes.value());
                
                List<String> list = new ArrayList<String>();
                while(it.hasNext()){
                    String str = it.next();
                    list.add("****" + str);
                }
                return list;
            }
        }).collect();
        
        for (String string : result) {
            System.out.println("result:" + string);
        }
        
        sc.close();
    }
    
    // foreachPartition
    public static void test2(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd5").setMaster("local[3]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        final Broadcast<String[]> signPrefixes = sc.broadcast(new String[]{"aaa", "bbb", "ccc"}); 
        
        JavaRDD<String> rdd = sc.textFile("file://e/tmp/test5/");
        
        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<String> paramT) throws Exception {
                getJdbcConn(signPrefixes.value());
                
            }
        });
        
        rdd.foreachAsync(new VoidFunction<String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(String paramT) throws Exception {
                getJdbcConn(signPrefixes.value());
            }
        });
        
//        rdd.foreach(new VoidFunction<String>() {
//            private static final long serialVersionUID = 1L;
//            @Override
//            public void call(String paramT) throws Exception {
//                getJdbcConn(signPrefixes.value());
//            }
//        });
        
        sc.close();
    }
    
    // 连接数据库
    private static void getJdbcConn(String[] config){
        System.out.println("======获取数据库连接" + Thread.currentThread().getName() + ":" + config);
    } 
}
