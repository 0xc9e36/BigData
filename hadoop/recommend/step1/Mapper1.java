package recommend.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author tan
 * @Description: ${todo}
 * @blog www.golangsite.com
 * @date 29/04/1809:07
 */
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		String userID = values[0];
		String itemID = values[1];
		String score = values[2];

		outKey.set(itemID);
		outValue.set(userID + "_" + score);
		context.write(outKey, outValue);
	}
}
