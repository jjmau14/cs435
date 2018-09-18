package cs435.pa1.Profile1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class Profile1Mapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        Text unigramText = new Text();
        Text noValue = new Text();

        while ( tokenizer.hasMoreTokens() ) {

            String unigram = tokenizer.nextToken();

            // If title of wiki - parse out just the title (remove doc id)
            if (unigram.contains("<===>")) {
                unigram = unigram.substring(unigram.lastIndexOf("<====>"));
            }

            // Remove all punctuation (anything thats no A-Z and 0-9)
            unigram = unigram.replaceAll("[^A-Za-z0-9]", "").toLowerCase();

            if (!unigram.equals("")) {
                unigramText.set(unigram);
                context.write(unigramText, noValue);
            }

        }

    }

}
