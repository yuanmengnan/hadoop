package org.apache.spark.ml.Example.chapter5.inputoutput;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

import lombok.Data;

/**
 * 第五章： 输入与输出
      5.2.1 Json文件 
 * @author yudan
 *
 */
public class JsonFileRead {
    public static void main(String[] args) {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        read();
    }
    
    public static void read(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MyMp3");
        JavaSparkContext jsc = new JavaSparkContext(conf);
                JavaRDD<String> input = jsc.textFile("file://E:/e/tmp/test5/json/input/1.json");
        JavaRDD<Mp3Info> result = input.mapPartitions(new ParseJson());
        result.foreach(x -> System.out.println(x));

        jsc.close();
    }
    
    public static class ParseJson implements FlatMapFunction<Iterator<String>,Mp3Info>{
        /**
         * 
         */
        private static final long serialVersionUID = 8603650874403773926L;

        //@Override
        public Iterable<Mp3Info> call(Iterator<String> lines) throws Exception {
            // TODO 自动生成的方法存根
            ArrayList<Mp3Info> mp3 = new ArrayList<Mp3Info>();
            ObjectMapper mapper = new ObjectMapper();
            while(lines.hasNext()){
                    String line = lines.next();
                try{
                    mp3.add(mapper.readValue(line, Mp3Info.class));
                }catch(Exception e){


                }       
            }
            return mp3;
        }

    }

    @Data
    public static class Mp3Info implements Serializable{
        private static final long serialVersionUID = -3811808269846588364L;
        private String name;
        private String album;
        private String path;
        private String singer;

        @Override
        public String toString() {
            return "Mp3Info [name=" + name + ", album=" 
                     + album + ", path=" + path + ", singer=" + singer + "]";
        }

    }
}



