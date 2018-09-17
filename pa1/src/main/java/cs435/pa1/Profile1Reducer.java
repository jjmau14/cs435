package cs435.pa1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

public class Profile1Reducer extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashSet<String> unigrams = new HashSet<String>();

        int count = 0;
        for (Text t: values) {
            unigrams.add(t.toString());
            count++;
            if (count > 500) break;
        }

        ArrayList<String> al = new ArrayList<String>(unigrams);
        //Collections.sort(al);

        for (String s : al)
            context.write(null, new Text(s));

    }
}

