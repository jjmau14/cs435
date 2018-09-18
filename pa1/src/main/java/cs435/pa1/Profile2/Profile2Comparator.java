package cs435.pa1.Profile2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Profile2Comparator extends WritableComparator{
    protected Profile2Comparator() {
        super(Text.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text k1 = (Text) w1;
        Text k2 = (Text) w2;

        return (k1.toString().split("\t")[0].compareTo(k2.toString().split("\t")[0]));

    }
}

