package org.apache.spark.ml.Example.chapter5.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;  

/**
 * 第五章： 输入与输出
      5.2　hive 

 * @author yudan
 *
 */
public class Hive {
    public static void main(String[] args) {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        read();
    }
    
    public static void read(){
        //local[n] 单机伪分布式模式，n个线程分别充当driver和Executors
        SparkConf conf = new SparkConf().setAppName("Simple Application yd5").setMaster("local[1]");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        HiveContext hiveCtx = new HiveContext(sc); 
        DataFrame rows = hiveCtx.sql("SELECT * FROM employee"); 
        Row firstRow = rows.first(); System.out.println(firstRow.getString(0)); // 字段0是name字
        
        sc.close();
    }
}



