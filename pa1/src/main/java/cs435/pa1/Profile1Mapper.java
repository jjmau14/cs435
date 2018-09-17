package cs435.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class Profile1Mapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Ensure only unique items
        HashSet<String> unigrams = new HashSet<String>();

        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while ( tokenizer.hasMoreTokens() )
            unigrams.add(tokenizer.nextToken().replaceAll("[^a-zA-Z ]", ""));

        for (String s : unigrams) {
            if (s.length() > 0) {
                context.write(new Text("key"), new Text(s));
            }
        }


    }


}
