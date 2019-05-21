package org.apache.spark.ml.Example;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

// JavaLambda 实现wordcount
public class JavaWordCount2 {
    public static void main(String[] args) throws IOException{
        //args[0]: hdfs://hadoop-1:8020/data/yearhot/data3.log
        //args[1]: hdfs://hadoop-1:8020/data/yearhot/data3_out
        if(args.length!=2){
            System.out.println("Usage JavaLambdaWordCount<input><output>");
            System.exit(1);
        }
       
        //upLocalToHdfs("C:\\Users\\Administrator\\Desktop\\learn1.jar", "/data/yearhot/learn3.jar");
        
        deleteOut(args);

        sparkExe(args);
    }
    
    private static void deleteOut(String[] args) throws IOException{
        String file = args[1];
        
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        Path path = new Path(file);  
        if(fs.exists(path)){
            System.out.println(fs.delete(path,true));  
        }
        fs.close();  
    }
    
    private static void sparkExe(String[] args){
       //SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(JavaWordCount2.class.getSimpleName());
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount2").setMaster("spark://hadoop-2:7077").set("spark.executor.memory", "512M");
        //sparkConf.set("spark.jars", "C:\\Users\\Administrator\\Desktop\\learn1.jar,");  //设置为本地jar
        sparkConf.set("spark.jars", "hdfs://hadoop-1:8020/data/yearhot/learn2.jar,");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //读取数据
        JavaRDD<String> jrdd = jsc.textFile(args[0]);

        //切割压平
        JavaRDD<String> jrdd2 = jrdd.flatMap(t -> Arrays.asList(t.split(" ")));
        //和1组合
        JavaPairRDD<String, Integer> jprdd = jrdd2.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
        //分组聚合
        JavaPairRDD<String, Integer> res = jprdd.reduceByKey((a, b) -> a + b);
        //保存
        res.saveAsTextFile(args[1]);
       //释放资源
        jsc.close();
    }
    
    public static Boolean upLocalToHdfs(String localFile, String hdfsFile)
            throws FileNotFoundException, IOException {

        String dst = hdfsFile;
        InputStream in = new BufferedInputStream(new FileInputStream(localFile));
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                //System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
        return true;
    }
}
