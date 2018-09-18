package cs435.pa1.Profile3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Profile3Maper_2 extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        Text unigramText = new Text();

        boolean first = true;

        while ( tokenizer.hasMoreTokens() ) {

            String unigram = tokenizer.nextToken();

            if (first) {
                unigramText.set(unigram);
                first = false;
                continue;
            }

            IntWritable frequency = new IntWritable(Integer.parseInt(unigram));
            context.write(frequency, unigramText);

        }

    }

}