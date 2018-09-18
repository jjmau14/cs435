package cs435.pa1.Profile2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Profile2Mapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        Text unigramText = new Text();
        String docId = "";

        while ( tokenizer.hasMoreTokens() ) {

            String unigram = tokenizer.nextToken();

            // If title of wiki - parse out just the title (remove doc id)
            if (unigram.contains("<====>")) {
                docId = unigram.substring(unigram.indexOf("<====>")+6, unigram.lastIndexOf("<====>"));
                unigram = unigram.substring(unigram.lastIndexOf("<====>"));
            }

            // Remove all punctuation (anything thats no A-Z and 0-9)
            unigram = unigram.replaceAll("[^A-Za-z0-9]", "").toLowerCase();

            if (!unigram.equals("")) {
                unigramText.set(unigram);
                String t = docId + "\t" + unigram;
                context.write(new Text(t), new IntWritable(1));
            }

        }

    }

}