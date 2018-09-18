package cs435.pa1.Profile3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Profile3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int frequency = 0;
        for (IntWritable i : values) {
            frequency += i.get();
        }

        context.write(key, new IntWritable(frequency));

    }
}
