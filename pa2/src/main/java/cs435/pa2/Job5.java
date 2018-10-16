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
            String[] sentences = value.toString().split("\\.");

            String docId = "";

            for (int i = 0; i < sentences.length; i++) {

                StringTokenizer itr = new StringTokenizer(sentences[i]);

                String sentence = "";

                while (itr.hasMoreTokens()) {

                    String token = itr.nextToken();

                    if (token.contains("<====>")) {
                        docId = token.substring(token.indexOf("<====>") + 6, token.lastIndexOf("<====>"));
                        token = token.substring(token.lastIndexOf("<====>"));
                    }

                    sentence = token.replaceAll("<====>", "") + " ";

                }

                if (!sentence.equals("")) {
                    context.write(new Text(docId), new Text("S" + sentence + "\t" + i));
                }
            }
        }

    }


    /**
     *  Reducer: maps U (unigrams) with S sentences (gets top 5 unigrams and top 3 sentences)
     */
    public static class Job5Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> sentences = new ArrayList<String>();
            HashMap<String, Double> unigramTF = new HashMap<String, Double>();

            for (Text val : values) {

                if (val.toString().charAt(0) == 'U') {

                    String token = val.toString().substring(1);
                    String[] unigramTFs = token.split("\t");

                    String unigram = unigramTFs[0];
                    Double tf = Double.parseDouble(unigramTFs[1]);

                    unigramTF.put(unigram, tf);

                } else {
                    sentences.add(val.toString().substring(1));
                }
            }

            TreeMap<Double, String> top3Sentences = new TreeMap<Double, String>();

            for (String s : sentences) {

                TreeMap<Double, String> top5Unigrams = new TreeMap<Double, String>();

                StringTokenizer itr = new StringTokenizer(s);

                while (itr.hasMoreTokens()) {

                    String unigram = itr.nextToken();
                    unigram = unigram.replaceAll("[^A-Za-z0-9]","").toLowerCase();

                    if (!unigram.equals("")) {

                        Double TF_IDF = -1.0;
                        if (unigramTF.containsKey(unigram)) {
                            TF_IDF = unigramTF.get(unigram);
                        }

                        if (!top5Unigrams.values().contains(unigram)) {

                            top5Unigrams.put(TF_IDF, unigram);

                            if (top5Unigrams.size() > 5) {
                                top5Unigrams.remove(top5Unigrams.firstKey());
                            }
                        }
                    }

                }

                Set<Double> top5values = top5Unigrams.keySet();
                Double TF_IDF = 0.0;

                for (Double val : top5values) {
                    TF_IDF += val;
                }

                top3Sentences.put(TF_IDF, s);

                if (top3Sentences.size() > 3) {
                    top3Sentences.remove(top3Sentences.firstKey());
                }
            }

            TreeMap<Double, String> sortOrder = new TreeMap<Double, String>();

            for (String s : top3Sentences.values()) {
                String[] idSentences = s.split("\t");
                sortOrder.put(Double.parseDouble(idSentences[1]), idSentences[0]);
            }


            Collection<String> orderedSentences = sortOrder.values();

            String concatSentences = "";
            for (String s : orderedSentences) {
                concatSentences += s + "\n\t";
            }

            context.write(key, new Text(concatSentences));

        }
    }

}