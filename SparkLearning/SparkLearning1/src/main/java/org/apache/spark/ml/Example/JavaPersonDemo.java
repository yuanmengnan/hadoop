package org.apache.spark.ml.Example;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author yudan
 * 
 *  1 SparkSQL概述（Overview）
    Spark SQL是Spark的一个组件，用于结构化数据的计算。Spark SQL提供了一个称为DataFrames的编程抽象，DataFrames可以充当分布式SQL查询引擎。
    
    2 DataFrames
    DataFrame是一个分布式的数据集合，该数据集合以命名列的方式进行整合。
    DataFrame可以理解为关系数据库中的一张表，也可以理解为R/Python中的一个data frame。
    DataFrames可以通过多种数据构造，例如：结构化的数据文件、hive中的表、外部数据库、Spark计算过程中生成的RDD等。
    DataFrame的API支持4种语言：Scala、Java、Python、R。
    
       参见：http://www.cnblogs.com/BYRans/p/5057110.html#21
 *
 */
public class JavaPersonDemo {
    
    public static void main(String[] args) {
        SparkConf  sparkConf = new SparkConf().setAppName("JavaPersonDemo").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        
        JavaRDD<String> rdd = context.parallelize(Arrays.asList("abc,2", "test,2", "test3,3", "test4,4"));
        JavaRDD<Person> personsRdd = rdd.map(str -> str.split(",")).map(s -> new Person(s[0], Integer.valueOf(s[1])));

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(context);
        DataFrame df = sqlContext.createDataFrame(personsRdd, Person.class);
        df.show();
        df.printSchema();
        
        System.out.println("all:");
        df.select("name", "age").show();
        
        System.out.println("age >=3:");
        df.filter("age >= 3").show();
        
        System.out.println("group by age:");
        df.groupBy("age").count().show();
        
        // 直接使用sql查询
        sqlContext.registerDataFrameAsTable(df, "person");
        sqlContext.sql("SELECT * FROM person WHERE age>1").show();
        List<Row> list = sqlContext.sql("SELECT * FROM person WHERE age>1").collectAsList();
        list.forEach(r -> System.out.println(r.get(0) + "==" + r.get(1)));
        for (Row row : list) {
            String name = row.getAs("name");
            int age = row.getAs("age");
            System.out.println(name + ":" + age);
        }
        context.close();
    }
    
    public static class Person implements Serializable {
        private static final long serialVersionUID = -6259413972682177507L;
        private String name;
        private int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String toString() {
            return name + ": " + age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

}
