package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by tan on 2018/4/17.
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * 在调用reduce方法之前有个shuffle过程
     * reduce()方法的输入数据来自于shuffle的输出，格式形如：<hello, {1,1,……}>
     */
    protected void reduce(Text key, Iterable<LongWritable> values, Mapper.Context context)
            throws IOException, InterruptedException {
        //定义一个累加计数器
        long counter = 0;

        //统计单词出现次数
        for (LongWritable value : values) {
            counter += value.get();
        }

        //输出key表示的单词的统计结果
        context.write(key, new LongWritable(counter));
    }

}