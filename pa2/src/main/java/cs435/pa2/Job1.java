package cs435.pa2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;

public class Job1 {

    public static class Job1Mapper extends Mapper<Object, Text, CompositeKey, IntWritable>{


        /**
         *  Map inputs to (DocumentID, Unigram), Frequency (1)
         * */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString());

            String docId = "";
            String unigram = "";

            while (tokenizer.hasMoreTokens()) {

                unigram = tokenizer.nextToken();

                // If title of wiki - parse out just the title (remove doc id)
                if (unigram.contains("<====>")) {
                    docId = unigram.substring(unigram.indexOf("<====>") + 6, unigram.lastIndexOf("<====>"));
                    unigram = unigram.substring(unigram.lastIndexOf("<====>"));
                }

                // Remove all punctuation (anything thats not A-Z and 0-9)
                unigram = unigram.replaceAll("[^A-Za-z0-9]", "").toLowerCase();

                if (!unigram.equals("") && !docId.equals("")) {
                    context.write(new CompositeKey(docId, unigram), new IntWritable(1));
                }

            }
        }
    }

    /**
     *  Same functionality as reducer but outputs composite keys instead of strings
     * */
    public static class Job1Combiner extends Reducer<CompositeKey, IntWritable, CompositeKey, IntWritable>{

        public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static class Job1Partitioner extends Partitioner<CompositeKey, IntWritable>{

        public int getPartition(CompositeKey key, IntWritable Value, int numPartitions){

            int hash = Math.abs(Integer.parseInt(key.docId.toString()));
            return hash % numPartitions;
        }
    }

    /**
     *  Writes: Document ID <TAB> Unigram <TAB> Frequency
     * */
    public static class Job1Reducer extends Reducer<CompositeKey, IntWritable, Text, IntWritable> {

        public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(new Text(key.toString()), new IntWritable(sum));
        }
    }

}