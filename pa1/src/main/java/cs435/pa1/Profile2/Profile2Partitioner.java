package cs435.pa1.Profile2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Profile2Partitioner extends Partitioner<Text, IntWritable> {

    public int getPartition(Text key, IntWritable Value, int numPartitions) {

        String partKey = key.toString().split("\t")[1].substring(0, 1);
        return (int) partKey.charAt(0) % numPartitions;
    }
}