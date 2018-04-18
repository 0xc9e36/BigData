package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 定义一个MapReduce程序的入口
 * 本类中有一个Job对象，该对象用于描述一个MapReduce作业，如Mapper类、Reducer类，以及map/reduce过程的输出类型等等
 */
public class WordCountRunner {
    static final String HDFS = "hdfs://localhost:8020";

    public static void main(String[] args) throws Exception {
        //实例化一个Job对象
        Configuration conf = new Configuration(); //加载配置文件
        Job job = Job.getInstance(conf);

        //设置Job作业所在jar包
        job.setJarByClass(WordCountRunner.class);

        //设置本次作业的Mapper类和Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置Mapper类的输出key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置Reducer类的输出key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定本次作业要处理的原始文件所在路径（注意，是目录）
        FileInputFormat.setInputPaths(job, new Path("/opt/input/wordcount"));
        //指定本次作业产生的结果输出路径（也是目录）
        FileOutputFormat.setOutputPath(job, new Path(HDFS + "/output/wordcount"));

        //提交本次作业，并打印出详细信息
        job.waitForCompletion(true);
    }
}
