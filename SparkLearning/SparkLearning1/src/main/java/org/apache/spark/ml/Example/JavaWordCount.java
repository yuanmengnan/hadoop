package org.apache.spark.ml.Example;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * User: hadoop
 * Date: 2014/10/10 0010
 * Time: 19:26
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

         if (args.length < 1) {
         System.err.println("Usage: JavaWordCount <file>");
         System.exit(1);
         }

        String filePath = args[0];
        //String filePath = "hdfs://hadoop-1:8020//data/yearhot/data.txt";
        // SparkConf sparkConf = new
        // SparkConf().setAppName("JavaWordCount").setMaster("local[4]");
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("spark://hadoop-2:7077")
                .set("spark.executor.memory", "512M");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(filePath, 1);

        JavaRDD<String> words = lines.flatMap(new Tu1());

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            // @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            // @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();
        System.out.println(output);
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + " ========= " + tuple._2());
        }
        ctx.stop();
    }

}