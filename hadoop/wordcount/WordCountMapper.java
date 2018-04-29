package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by tan on 2018/4/17.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * 在执行Mapper任务之前，MapReduce框架中有一个组件（FileInputFormat的一个实例对象）会读取文件中的一行，将这一行的起始偏移量和行内容封装成key-value，作为参数传入map()方法。
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //拿到一行的内容
        String line = value.toString();
        //将行内容切分成单词数组
        String[] words = StringUtils.split(line, ' ');

        for (String word : words) {
            //输出  <单词,log4j.properties> 这样的key-value对
            context.write(new Text(word), new LongWritable(1L));
        }
    }
}
