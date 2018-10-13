package cs435.pa2;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Set;

public class Job2 {

    public static class Job2Mapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String docId = "";
            String unigram = "";
            String frequency = "";

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                docId = itr.nextToken();
                unigram = itr.nextToken();
                frequency = itr.nextToken();
                context.write(new Text(docId), new Text(unigram + "\t" + frequency));
            }
        }
    }

    /**
     * Job 2 Reducer:
     * 1. Figure out the max raw frequency of any term in the article
     * 2. Compute the TF for each term using the max
     * Output: < DocumentID, {Unigram \t TFvalue}
     */
    public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Set<String> docIds = new HashSet<String>();

            if (!docIds.contains(key.toString())) {

                // Increment job counter for number of docIds
                docIds.add(key.toString());
                context.getCounter(AllJobs.DocumentIdCounter.numDocIds.N).increment(1);

            }

            int maxFrequency = 0;

            ArrayList<Text> keyValues = new ArrayList<Text>();

            for (Text val : values) {

                String[] unigramFrequency = val.toString().split("\t");

                int frequency = Integer.parseInt(unigramFrequency[1]);

                if(frequency > maxFrequency){
                    maxFrequency = frequency;
                }

                keyValues.add(new Text(val.toString()));

            }

            for(Text val : keyValues) {

                String[] unigramFrequency = val.toString().split("\t");

                String unigram = unigramFrequency[0];
                int frequency = Integer.parseInt(unigramFrequency[1]);

                double termFrequency = 0.5 + 0.5 * (frequency/maxFrequency);

                context.write(key, new Text(unigram + "\t" + termFrequency));

            }
        }
    }

}