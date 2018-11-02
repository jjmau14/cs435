package cs435.pa2;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Job5 {

    /**
     *  Map inputs to DocumentID, (Unigram, TermFrequency)
     * */
    public static class Job5MapperClass1 extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

            String docID = "";
            String unigram = "";
            String termFrequency = "";
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                docID = itr.nextToken();
                unigram = "U" + itr.nextToken();
                termFrequency = itr.nextToken();
                context.write(new Text(docID), new Text(unigram + "\t" + termFrequency));
            }
        }
    }

    /**
     *  Maps inputs to sentences (split on .)
     * */
    public static class Job5MapperClass2 extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Split by sentences
            String[] sentences = value.toString().split("\\. ");

            String docId = "";

            for (int i = 0; i < sentences.length; i++) {

                StringTokenizer itr = new StringTokenizer(sentences[i]);

                String sentence = "S";

                while (itr.hasMoreTokens()) {

                    String token = itr.nextToken();

                    if (token.contains("<====>")) {
                        docId = token.substring(token.indexOf("<====>") + 6, token.lastIndexOf("<====>"));
                        token = token.substring(token.lastIndexOf("<====>"));
                    }

                    sentence += token.replaceAll("<====>", "") + " ";

                }

                if (!sentence.equals("S") && !docId.equals("")) {
                    context.write(new Text(docId), new Text(sentence + "\t" + i));
                }
            }
        }

    }


    public static class Job5Partitioner extends Partitioner<Text, Text>{

        public int getPartition(Text key, Text Value, int numPartitions){

            int hash = Math.abs(key.toString().hashCode());
            return hash % numPartitions;
        }
    }

    /**
     *  Reduces Unigrams, Frequency and multi input sentences to top 3 sentences
     *  based on top 5 unigrams
     */
    public static class Job5Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            final ArrayList<String> sentences = new ArrayList<String>();
            final HashMap<String, Double> unigramTF_IDFs = new HashMap<String, Double>();

            for (Text val : values) {

                if (val.toString().charAt(0) == 'U') {

                    String unigram = val.toString().substring(1);
                    String[] unigramTF_IDF = unigram.split("\t");

                    unigramTF_IDFs.put(unigramTF_IDF[0], Double.parseDouble(unigramTF_IDF[1]));

                } else {

                    sentences.add(val.toString().substring(1));

                }

            }

            TreeMap<Double, String> top3 = new TreeMap<Double, String>();

            for (String sentence : sentences) {

                TreeMap<Double, String> top5 = new TreeMap<Double, String>();

                StringTokenizer itr = new StringTokenizer(sentence);

                while (itr.hasMoreTokens()) {

                    String unigram = itr.nextToken();
                    unigram = unigram.replaceAll("[^A-Za-z0-9]","").toLowerCase();

                    if (unigram.equals("")) {
                        continue;
                    }

                    double TF_IDF = -1.0;
                    if (unigramTF_IDFs.containsKey(unigram)) {
                        TF_IDF = unigramTF_IDFs.get(unigram);
                    }

                    if (!top5.values().contains(unigram)) {
                        top5.put(TF_IDF, unigram);

                        if (top5.size() > 5) {
                            top5.remove(top5.firstKey());
                        }

                    }
                }

                Set<Double> top5values = top5.keySet();
                double SentenceTF_IDF = 0.0;

                for (Double val : top5values) {
                    SentenceTF_IDF += val;
                }

                top3.put(SentenceTF_IDF, sentence);

                if (top3.size() > 3) {
                    top3.remove(top3.firstKey());
                }

            }

            TreeMap<Double, String> sentenceOrder = new TreeMap<Double, String>();

            for (String s : top3.values()) {

                String[] sentenceValues = s.split("\t");
                sentenceOrder.put(Double.parseDouble(sentenceValues[1]), sentenceValues[0]);

            }

            Collection<String> orderedSentences = sentenceOrder.values();
            String summary = "";

            for (String s : orderedSentences) {
                summary += s + "\n";
            }

            if (summary.length() > 1)
                context.write(key, new Text(summary));

        }
    }

}