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

        /**
         *  Map inputs to DocumentID, (Unigram, Frequency)
         * */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String docId = "";
            String unigram = "";
            String frequency = "";

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                docId = itr.nextToken();
                unigram = itr.nextToken();
                frequency = itr.nextToken();
                context.write(new Text(docId), new Text(unigram + ',' + frequency));
            }
        }

    }

    public static class Job2Partitioner extends Partitioner<Text, Text>{

        public int getPartition(Text key, Text Value, int numPartitions){

            int hash = Math.abs(key.toString().hashCode());
            return hash % numPartitions;

        }
    }

    /**
     *  Writes: Document ID <TAB> Unigram <TAB> TermFrequency
     * */
    public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            final Set<String> docIds = new HashSet<String>();

            if (!docIds.contains(key.toString())) {
                docIds.add(key.toString());
                context.getCounter(AllJobs.DocumentIdCounter.numDocIds.N).increment(1);
            }

            double maxFrequency = -1.0;

            final ArrayList<Text> store = new ArrayList<Text>();

            for (final Text val : values) {

                final String valText = val.toString();

                store.add(new Text(valText));

                String[] unigramFrequency = valText.split(",");
                double frequency = Integer.parseInt(unigramFrequency[1]);

                if (frequency > maxFrequency) {
                    maxFrequency = frequency;
                }

            }

            for (final Text val : store) {

                final String valText = val.toString();

                String[] unigramFrequency = valText.split(",");
                String unigram = unigramFrequency[0];

                double frequency = Integer.parseInt(unigramFrequency[1]);
                double TF = 0.5 + 0.5 * (frequency/maxFrequency);

                context.write(key, new Text(unigram + "\t" + TF));
            }
        }

    }

}