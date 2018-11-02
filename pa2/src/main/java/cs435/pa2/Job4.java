package cs435.pa2;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Job4 {

    /**
     *  Map input to DocumentID, (Unigram, TF_IDF)
     * */
    public static class Job4Mapper extends Mapper<Object, Text, Text, Text> {

        private long N;

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.N  = context.getConfiguration().getLong(AllJobs.DocumentIdCounter.numDocIds.N.name(), 0);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String docID = "";
            String unigram = "";
            String termFrequency = "";
            String n = "";

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {

                docID = itr.nextToken();
                unigram = itr.nextToken();
                termFrequency = itr.nextToken();
                n = itr.nextToken();

                double IDF = Math.log10( this.N / Double.parseDouble(n) );

                double TF_IDF = Double.parseDouble(termFrequency) * IDF;

                context.write(new Text(docID), new Text(unigram + "\t" + TF_IDF));
            }
        }

    }

    public static class Job4Partitioner extends Partitioner<Text, Text>{

        public int getPartition(Text key, Text Value, int numPartitions){

            int hash = Math.abs(Integer.parseInt(key.toString()));
            return hash % numPartitions;
        }
    }

    /**
     *  Same as mapper
     * */
    public static class Job4Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val: values) {
                String[] unigramTF_IDF = val.toString().split("\t");
                context.write(key, new Text(unigramTF_IDF[0] + "\t" + unigramTF_IDF[1]));
            }
        }

    }

}