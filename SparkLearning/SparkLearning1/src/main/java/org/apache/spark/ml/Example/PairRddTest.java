package org.apache.spark.ml.Example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 第四章： 键值对操作
      
 * @author yudan
 *
 */
public class PairRddTest {
    public static void main(String[] args) {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        //test1();
       
        //test3();
        
        test4();
       
    }

    /**
     *  parallelizePairs()： 创建 ParisRDD
     *  groupByKey()：按键进行分组
     *  reduceByKey()：合并相同的键
     */
    public static void test1(){
        SparkConf conf = new SparkConf().setAppName("Simple Application yd22").setMaster("local[*]");  //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        //Function2<AvgCount, Integer, AvgCount> addAndCount = (AvgCount avgCount2, Integer num2) => {return avgCount2;};
        //SparkConf conf = new SparkConf().setAppName("Simple Application yd3").setMaster("spark://hadoop-2:7077").set("spark.executor.memory", "512M");
        //conf.set("spark.jars", "C:\\Users\\secneo\\Desktop\\learn11.jar,");   //设置为本地jar
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        //插入数据
        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        list.add(new Tuple2<Integer, Integer>(1, 2));
        list.add(new Tuple2<Integer, Integer>(1, 3));
        list.add(new Tuple2<Integer, Integer>(3, 5));
        list.add(new Tuple2<Integer, Integer>(2, 4));
        list.add(new Tuple2<Integer, Integer>(2, 1));
        
        JavaPairRDD<Integer, Integer> pair = sc.parallelizePairs(list);
        System.out.println("原始rdd:" + pair.collect());
        
        JavaPairRDD<Integer, Iterable<Integer>> after_group = pair.groupByKey();
        System.out.println("groupByKey结果：" + after_group.collect());

        JavaPairRDD<Integer, Integer> pair2 = pair.reduceByKey((x, y) -> (x * y));
        System.out.println("reduceByKey结果：" + pair2.collect());
        sc.stop();
    }
    
    public static void test2(){
        SparkConf conf = new SparkConf().setAppName("Simple Application yd22").setMaster("local[1]");  //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        //Function2<AvgCount, Integer, AvgCount> addAndCount = (AvgCount avgCount2, Integer num2) => {return avgCount2;};
        //SparkConf conf = new SparkConf().setAppName("Simple Application yd3").setMaster("spark://hadoop-2:7077").set("spark.executor.memory", "512M");
        //conf.set("spark.jars", "C:\\Users\\secneo\\Desktop\\learn11.jar,");   //设置为本地jar
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> list = sc.textFile("hdfs://hadoop-1:8020/data/yearhot/data3.log");
        JavaPairRDD<String, String> pair = list.mapToPair(new PairFunction<String, String, String>(){
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(String x) throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2<String, String>(x.split(" ")[0], x);
            }
        });
        
        JavaPairRDD<String, Iterable<String>> after_group = pair.groupByKey();
    }
    
    // 使用combineByKey 进行聚合, 注意分区与local[N]中的N有关
    public static void test3(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd22").setMaster("local[1]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        //插入数据
        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
        list.add(new Tuple2<String, Integer>("a", 11));
        list.add(new Tuple2<String, Integer>("a", 3));
        list.add(new Tuple2<String, Integer>("c", 5));
        list.add(new Tuple2<String, Integer>("b", 4));
        list.add(new Tuple2<String, Integer>("b", 2));
        
        // 测试：flatMapValues
        JavaPairRDD<String, Integer> pair0 = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> pair = pair0.flatMapValues(x ->  {List<Integer> li = new ArrayList<Integer>(); li.add(x); li.add(x+2); return li;});
        
//        // 测试：对值进行计算 ，不改变键
//        pair = pair.mapValues(x -> (x+1));
        
//        // 测试: 按键排序 
//        JavaPairRDD<String, Integer> pair2 = pair.sortByKey(false);
//        pair2.saveAsTextFile("hdfs://hadoop-1:8020/data/yearhot/test3.log");
        
        // 分区内key的累加器未创建时，创建key对应累加器
        // Function : 参数1， 返回值
        Function<Integer, AvgCount> createAcc = new Function<Integer, PairRddTest.AvgCount>() {
            private static final long serialVersionUID = 8502409282047946176L;

            @Override
            public AvgCount call(Integer x) throws Exception {
                System.out.println("分区内创建累加器：" + x);
                return new AvgCount(x, 1);
            }
        };
        
        // 同一分区内累加
        // Function2: 参数1， 参数2， 返回值
        Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<PairRddTest.AvgCount, Integer, PairRddTest.AvgCount>() {
            private static final long serialVersionUID = -1976641444735699841L;

            @Override
            public AvgCount call(AvgCount a, Integer x) throws Exception {
                System.out.println("分区内累加：" + x);
                a.total += x;
                a.num ++;
                return a;
            }
        }; 
        
        // 将所有分区的结果合并
        Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<PairRddTest.AvgCount, PairRddTest.AvgCount, PairRddTest.AvgCount>() {
            private static final long serialVersionUID = -3820336588435058033L;
            
            @Override
            public AvgCount call(AvgCount a, AvgCount b) throws Exception {
                System.out.println("合并分区：" + a.total + "--" + b.total);
                a.total += b.total;
                a.num += b.num;
                return a;
            }
        };
        
        // AvgCount init = new AvgCount(0, 0);
        JavaPairRDD<String, AvgCount> avgCounts = pair.combineByKey(createAcc, addAndCount, combine);

        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        
        for (Entry<String, AvgCount> e: countMap.entrySet()) {
            System.out.println(e.getKey() + "----avg:" + e.getValue().avg() + "----total:" + e.getValue().total);
        }
    }
    
    public static class AvgCount implements Serializable{
        private static final long serialVersionUID = 1019778776851309798L;
        
        private int total;
        private int num;
        
        public double avg(){
            return this.total / num;
        }

        public AvgCount(int total, int num) {
            super();
            this.num = num;
            this.total = total;
        }
        
    }
    
    /**
     * 分区数、并行度调优
     * 设置并行度：parallelizePairs(*, n)
     * 查看并行度：rdd.partitions().size()
     * 执行时设置并行度：reduceByKey(Function, n)
     */
    public static void test4(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd22").setMaster("local[30]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        //插入数据
        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
        list.add(new Tuple2<String, Integer>("a", 11));
        list.add(new Tuple2<String, Integer>("a", 3));
        list.add(new Tuple2<String, Integer>("c", 5));
        list.add(new Tuple2<String, Integer>("b", 4));
        list.add(new Tuple2<String, Integer>("b", 2));
        
        // 测试：flatMapValues
        JavaPairRDD<String, Integer> pair0 = sc.parallelizePairs(list, 2);         //自定义并行度
        JavaPairRDD<String, Integer> pair = pair0.reduceByKey((x, y) ->(x+y), 10); //自定义并行度
        System.out.println("partitions:" + pair.partitions().size());
        System.out.println(pair.collectAsMap());
        
        System.out.println(pair0.sortByKey(false).collect());
    }
    
    
}



