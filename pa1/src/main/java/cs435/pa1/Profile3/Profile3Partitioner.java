package cs435.pa1.Profile3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Profile3Partitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {

        String partKey = key.toString().substring(0,1);

        if (partKey.charAt(0) >= '0' && partKey.charAt(0) <= '9') {
            return (int)partKey.charAt(0) % 10;
        }

        // Hash of Key % of R -> save 0 partition for integers
        return ((int)partKey.charAt(0) % 'a') + 10;

    }

}