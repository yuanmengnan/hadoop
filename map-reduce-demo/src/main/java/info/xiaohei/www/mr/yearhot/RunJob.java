package info.xiaohei.www.mr.yearhot;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author yudan
 * mr例子：
 *  1、计算在1949-1955年，每年温度最高的时间。
    2、计算在1949-1955年，每年温度最高前十天。
 */

public class RunJob {
    public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static class HotMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
        Map<Long, String> map = new HashMap<Long, String>();
        
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            map.put(key.get(), value.toString());
            System.out.println(Thread.currentThread() + "单步map:" + map.toString());
            
            String line = value.toString();
            System.out.println("line=" + line);
            System.out.println("℃");
            System.out.println("---------------------------------------");
            String[] ss = line.split("/t");
            System.out.println("ss=" + ss.length);
            if (ss.length == 2) {
                try {
                    Date date = SDF.parse(ss[0]);
                    System.out.println(date);
                    Calendar c = Calendar.getInstance();
                    c.setTime(date);
                    int year = c.get(1);
                    System.out.println("ss[1]" + ss[1]);
                    String hot = ss[1].substring(0, ss[1].indexOf("℃"));
                    System.out.print("hot=" + hot);
                    KeyPair kp = new KeyPair();
                    kp.setYear(year);
                    kp.setHot(Integer.parseInt(hot));
                    context.write(kp, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

    static class HotReduce extends Reducer<KeyPair, Text, KeyPair, Text> {
        Map<KeyPair, Iterable<Text>> map = new HashMap<KeyPair, Iterable<Text>>();
        
        protected void reduce(KeyPair kp, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            map.put(kp, value);
            System.out.println(Thread.currentThread() + "单步reduce:" + map.toString());
            System.out.println(kp.toString());
            for (Text v : value)
                context.write(kp, v);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJobName("hot");
            job.setJarByClass(RunJob.class);
            job.setMapperClass(HotMapper.class);
            job.setReducerClass(HotReduce.class);
            job.setMapOutputKeyClass(KeyPair.class);
            job.setMapOutputValueClass(Text.class);

            job.setNumReduceTasks(5);   // reduce数量:默认1
            job.setPartitionerClass(FirstPartition.class);    //HashPartitioner<K, V>
            job.setSortComparatorClass(SortHot.class);
            job.setGroupingComparatorClass(GroupHot.class);

            Path inPath = new Path("hdfs://hadoop-1:8020/data/yearhot/*.txt");
            Path outPath = new Path("hdfs://hadoop-1:8020/out/yearhot/");
            FileInputFormat.addInputPath(job, inPath);
            FileOutputFormat.setOutputPath(job, outPath);
            
            FileSystem fs = outPath.getFileSystem(new Configuration());
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}