package temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Group extends WritableComparator {

    public Group() {
        super(keyPair.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        keyPair k1 = (keyPair) a;
        keyPair k2 = (keyPair) b;
        return Integer.compare(k1.getYear(), k2.getYear());
    }

}
