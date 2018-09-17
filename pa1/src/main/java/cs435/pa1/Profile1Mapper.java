package cs435.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class Profile1Mapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Ensure only unique items
        HashSet<String> unigrams = new HashSet<String>();

        for (String s : value.toString().split(" "))
            unigrams.add(s);

        int count = 0;
        for (String s : unigrams) {
            context.write(new Text(s), new IntWritable(1));
            count++;
            if (count > 500) break;
        }


    }


}
