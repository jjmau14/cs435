package cs435.pa1.Profile2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Profile2Reducer_2 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(new Text(key.toString().split("\t")[0]), new Text(val.toString() + "\t" + key.toString().split("\t")[1]));
        }
    }
}