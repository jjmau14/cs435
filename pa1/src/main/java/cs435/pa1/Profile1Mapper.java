package cs435.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class Profile1Mapper extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Ensure only unique items
        HashSet<String> unigrams = new HashSet<String>();

        for (String s : value.toString().split(" "))
            unigrams.add(s);

        int count = 0;
        for (String s : unigrams) {
            if (s.length() > 0) {
                int k = s.charAt(0) < 'a' ? 0 :  s.charAt(0) < 'z' ? 1 : 2;
                context.write(new IntWritable(k), new Text());
            }
        }


    }


}
