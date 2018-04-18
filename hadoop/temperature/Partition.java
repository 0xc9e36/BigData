package temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import temperature.keyPair;

public class Partition extends Partitioner<keyPair, Text> {

    @Override
    public int getPartition(keyPair key, Text value, int num) {
        return (key.getYear() * 127) % num;
    }
}
