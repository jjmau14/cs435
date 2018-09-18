package cs435.pa1.Profile2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Profile2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int frequency = 0;

        for (IntWritable val : values) {
            frequency += val.get();
        }

        // DOCID | Unigram
        context.write(key, new IntWritable(frequency));
    }
}
