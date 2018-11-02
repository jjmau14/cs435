package cs435.pa2;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;

public class Job3 {

    /**
     *  Map inputs to Unigram, (DocumentID, TermFrequency)
     * */
    public static class Job3Mapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {

            String docID = "";
            String unigram = "";
            String termFrequency = "";
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                docID = itr.nextToken();
                unigram = itr.nextToken();
                termFrequency = itr.nextToken();
                context.write(new Text(unigram), new Text(docID + "\t" + termFrequency));
            }
        }

    }

    public static class Job3Partitioner extends Partitioner<Text, Text>{

        public int getPartition(Text key, Text Value, int numPartitions){

            int hash = Math.abs(key.toString().hashCode());
            return hash % numPartitions;
        }
    }


    /**
     *  Writes Document ID <TAB> unigram <TAB> Frequency <TAB> Ni
     * */
    public static class Job3Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<Text> allUnigrams = new ArrayList<Text>();

            for (Text val: values) {
                allUnigrams.add(new Text(val.toString()));
            }

            int n = allUnigrams.size();

            for (Text val: allUnigrams) {

                String[] docIdFrequency = val.toString().split("\t");

                String docId = docIdFrequency[0];
                String frequency = docIdFrequency[1];

                context.write(new Text(docId), new Text(key + "\t" + frequency + "\t" + n));
            }
        }

    }

}