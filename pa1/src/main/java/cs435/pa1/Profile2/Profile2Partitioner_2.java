package cs435.pa1.Profile2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Profile2Partitioner_2 extends Partitioner<Text, Text> {

    public int getPartition(Text key, Text value, int numPartitions){

        return Integer.parseInt(key.toString().split("\t")[0]) % numPartitions;
    }
}