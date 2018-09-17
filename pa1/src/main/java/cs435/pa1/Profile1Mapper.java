package cs435.pa1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class Profile1Mapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Ensure only unique items
        HashSet<String> unigrams = new HashSet<String>();

        for (String s : value.toString().split(" "))
            unigrams.add(s);

        int count = 0;
        for (String s : unigrams) {
            if (s.length() > 0) {
                context.write(new Text(s.substring(0, 1)), new Text(s));
                count++;
                if (count > 500) break;
            } else if (s.length() == 1) {
                context.write(new Text(s), new Text(s));
                count++;
                if (count > 500) break;
            }
        }


    }


}
