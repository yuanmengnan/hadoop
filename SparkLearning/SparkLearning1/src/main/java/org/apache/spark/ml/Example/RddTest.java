package org.apache.spark.ml.Example;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * 1，测试或实验性质的本地运行模式 （单机）
    该模式被称为Local[N]模式，是用单机的多个线程来模拟Spark分布式计算，通常用来验证开发出来的应用程序逻辑上有没有问题。
    其中N代表可以使用N个线程，每个线程拥有一个core。如果不指定N，则默认是1个线程（该线程有1个core）。
    如果是local[*]，则代表 Run Spark locally with as many worker threads as logical cores on your machine.
    
 * @author yudan
 *
 */
public class RddTest {
    private static JavaSparkContext sc;


    public static void main(String[] args) {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        test2();
    }
    
    public static void test1(){
        SparkConf conf = new SparkConf().setAppName("Simple Application yd11").setMaster("local[4]");
        
        sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        System.out.println(distData.count());

        sc.stop();
    }
    
    /**
     * map():
     * persist():
     * fold():
     * reduce():
     * aggregate():
     */
    public static void test2(){
        SparkConf conf = new SparkConf().setAppName("Simple Application yd22").setMaster("local[*]");  //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd0 = sc.parallelize(data);
        
        JavaRDD<Integer> rdd = rdd0.map((Integer x) ->{ System.out.println("-----map:"+ x);  return (x + 1);});
        rdd.persist(StorageLevel.MEMORY_ONLY());    //持久化（缓存）
        
        // fold(), 返回值必须与RDD中相同
        Integer total1 = rdd.fold(0, (Integer x, Integer y) -> (x+y));
        System.out.println("fold total:" + total1);
        
        // reduce, 返回值必须与RDD中相同
        Integer total2 = rdd.reduce((Integer x, Integer y) -> (x+y));
        System.out.println("reduce total:" + total2);
        
        AvgCount initial = new AvgCount(0, 0); 
        
        // aggregate, 返回值可以与RDD中不同
        AvgCount result = rdd.aggregate(initial, 
                (AvgCount avg, Integer num) -> {System.out.println(num); avg.total += num; avg.num += 1;  return avg;}, 
                (AvgCount avg1, AvgCount avg2) -> {avg1.total += avg2.total;  avg1.num += avg2.num; return avg1;}
                );
        System.out.println(result.total);
        System.out.println(result.num);
        System.out.println(result.avg());
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
}