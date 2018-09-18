package cs435.pa1;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class Profile1Partitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {

        String partKey = key.toString().substring(0,1);

        if (partKey.charAt(0) >= '0' && partKey.charAt(0) <= '9') {
            return 0;
        }

        // Hash of Key % of R -> save 0 partition for integers
        return ((Integer.parseInt(partKey) - 10) % numPartitions) + 1;

    }

}
