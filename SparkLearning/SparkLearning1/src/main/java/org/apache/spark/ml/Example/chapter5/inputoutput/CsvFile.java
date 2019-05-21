package org.apache.spark.ml.Example.chapter5.inputoutput;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;



/**
 * 第五章： 输入与输出
      5.2.1 Json文件 
 * @author yudan
 *
 */
public class CsvFile {
    public static void main(String[] args) throws IOException {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        createCsv();
        
        read();
    }
    
    public static void createCsv() throws IOException{
        CSVWriter writer =new CSVWriter(new FileWriter("E:/e/tmp/test5/csv/1.csv"),'\t');
        List<String[]> lines=new ArrayList<String[]>();
        lines.add(new String []{"hhh","ggg","hhh"});
        lines.add(new String[]{"xxx","yyy","zzz"});
        writer.writeAll(lines);
        writer.close();
    }
    
    public static void read(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CsvFile");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //读取csv文件，以行为单位，保存在lines中
        JavaRDD<String> lines = jsc.textFile("file://E:/e/tmp/test5/csv/1.csv");
        
        //定义如何将一行中的元素读取出来，以String[]的形式返回
        class ParseLine  implements  Function<String,String[]>{

            private static final long serialVersionUID = 1L;
            private CSVReader reader;

            public String[] call(String s) throws Exception {
                reader = new CSVReader(new StringReader(s),'\t');
                //以数组的形式返回每一行中的元素
                return reader.readNext();
            }
        }
        
        //利用ParseLine，转化处理lines
        JavaRDD<String[]> results = lines.map(
               new ParseLine()
        );
        //遍历输出results中的内容
        for(String  s []: results.collect() ){
            System.out.println("this is the elements of one line!");
            for(String str:s)
                System.out.println(str);
        }
       
        jsc.close();
    }
    

}



