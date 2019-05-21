package org.apache.spark.ml.Example.chapter5.inputoutput;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 《Spark快速大数据分析》
 * 第五章： 输入与输出
      5.2 Sequence文件 
      
      SequenceFile 是由没有相对关系结构的键值对文件组成的常用 Hadoop 格式。
      SequenceFile 文件有同步标记，Spark 可以用它来定位到文件中的某个点，然后再与记录的边界对 齐。
            这可以让 Spark 使用多个节点高效地并行读取 SequenceFile 文件。
      SequenceFile 也是 Hadoop MapReduce 作业中常用的输入输出格式，所以如果你在使用一个已有的 Hadoop系统，数据很有可能是以 SequenceFile的格式供你使用的。
           由于 Hadoop 使用了一套自定义的序列化框架，因此 SequenceFile 是由实现 Hadoop 的 Writable 接口的元素组成

 * @author yudan
 *
 */
public class SequenceFile {
    
    public static void main(String[] args) throws IOException {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        read();
    }
    
    
    public static void write(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CsvFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

      //新建PairRDD<String,Integer>
        JavaPairRDD<String,Integer> data = sc
                .parallelizePairs(
                        Arrays.asList(new Tuple2<String, Integer>("zhouyang", 1),
                        new Tuple2<String, Integer>("jack", 2),
                        new Tuple2<String, Integer>("bob", 3)));
        //将PairRDD<String,Integer> 转换为hadoop io中对应的格式 PairRDD<Text,IntWritable> 
        JavaPairRDD<Text,IntWritable>  result =data.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Text, IntWritable>() {
                    private static final long serialVersionUID = 1L;

                    public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) throws Exception {
                        return new Tuple2(new Text(record._1()),new IntWritable(record._2()));
                    }
                }
        );
        
        //将result以SequenceFile保存在指定目录下
        result.saveAsHadoopFile("file:///E:/e/tmp/test5/sequence/out/1.seq", Text.class,IntWritable.class,  SequenceFileOutputFormat.class);
       
        sc.close();
    }
    
    public static void read(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CsvFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaPairRDD<Text,IntWritable> input= sc.sequenceFile("file:///E:/e/tmp/test5/sequence/out/1.seq",Text.class,IntWritable.class);
        JavaPairRDD<String,Integer> results =input.mapToPair(
                new PairFunction<Tuple2<Text, IntWritable>, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) throws Exception {
                        return new Tuple2<String,Integer>(record._1().toString(),record._2().get());
                    }
                }
        );
        for(Tuple2<String,Integer> tuple: results.collect())
            System.out.println(tuple._1()+"->" +tuple._2());
        
        sc.close();
    }
}
