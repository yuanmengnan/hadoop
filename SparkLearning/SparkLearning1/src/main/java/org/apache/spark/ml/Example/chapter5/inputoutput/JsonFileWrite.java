package org.apache.spark.ml.Example.chapter5.inputoutput;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.ml.Example.chapter5.inputoutput.JsonFileRead.Mp3Info;
import org.apache.spark.ml.Example.chapter5.inputoutput.JsonFileRead.ParseJson;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 第五章： 输入与输出
      5.2.1 Json文件 
 * @author yudan
 *
 */
public class JsonFileWrite {
    public static void main(String[] args) {
        String HADOOP_USER_NAME = System.getenv().get("HADOOP_USER_NAME");
        System.out.println(HADOOP_USER_NAME);
        
        write();
    }
    
    public static void write(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MyMp3");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        JavaRDD<String> input = jsc.textFile("file://E:/e/tmp/test5/json/input/1.json");
        System.out.println(input.count());
        JavaRDD<Mp3Info> result = input.mapPartitions(new ParseJson()).
                                      filter(
                                          x->x.getAlbum().equals("怀旧专辑")
                                      );
        
        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
        result.foreach(x->System.out.println(x));
        formatted.saveAsTextFile("file:///E:/e/tmp/test5/json/output");

        jsc.close();
    }
    
    public static class WriteJson implements FlatMapFunction<Iterator<Mp3Info>, String> {
        /**
         * 
         */
        private static final long serialVersionUID = -6590868830029412793L;

        public Iterable<String> call(Iterator<Mp3Info> song) throws Exception {
            ArrayList<String> text = new ArrayList<String>();
            ObjectMapper mapper = new ObjectMapper();
            while (song.hasNext()) {
                Mp3Info person = song.next();
                text.add(mapper.writeValueAsString(person));
            }
            return text;
        }
    }

}



