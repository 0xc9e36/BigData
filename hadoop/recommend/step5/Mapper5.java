package recommend.step5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author tan
 * @Description: ${todo}
 * @blog www.golangsite.com
 * @date 29/04/1812:46
 */
public class Mapper5 extends Mapper<LongWritable, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();

	private List<String> cacheList = new ArrayList<>();
	private DecimalFormat df = new DecimalFormat("0.00");

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		FileReader fr = new FileReader("recommend");
		BufferedReader br = new BufferedReader(fr);

		String line = null;
		while ((line = br.readLine()) != null) {
			cacheList.add(line);
		}

		br.close();
		fr.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { ;
		String item_matrix1 = value.toString().split("\t")[0];
		String[] user_score_array_matrix1 = value.toString().split("\t")[1].split(",");

		for (String line : cacheList) {
			String item_matrix2 = line.toString().split("\t")[0];
			String[] user_score_array_matrix2 = line.toString().split("\t")[1].split(",");

			//物品ID相同
			if (item_matrix1.equals(item_matrix2)) {
				for (String user_score_matrix1 : user_score_array_matrix1) {
					boolean flag = false;
					String user_matrix1 = user_score_matrix1.split("_")[0];
					String score_matrix1 = user_score_matrix1.split("_")[1];

					for (String user_score_matrix2 : user_score_array_matrix2) {
						String user_matrix2 = user_score_matrix2.split("_")[0];
						if (user_matrix1.equals(user_matrix2)) {
							flag = true;
						}
					}

					if (false == flag) {
						outKey.set(user_matrix1);
						outValue.set(item_matrix1 + "_" + score_matrix1);
						context.write(outKey, outValue);
					}
				}
			}
		}
	}
}
