package cs435.pa1.Profile2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Profile2Mapper_2 extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context )
            throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());

        boolean isDocId = true;
        boolean isUnigram = false;
        String docId = "";
        String unigramText = "";
        String frequency = "";

        while (itr.hasMoreTokens()) {
            String unigram = itr.nextToken();
            if(isDocId){
                docId = unigram;
                isUnigram = true;
                isDocId = false;

            }
            else if(isUnigram){
                unigramText = unigram;
                isUnigram = false;
            }
            else {
                frequency = unigram;
                context.write(new Text(docId + "\t" + frequency), new Text(unigramText));
                isDocId = true;
                isUnigram = false;
            }
        }
    }
}