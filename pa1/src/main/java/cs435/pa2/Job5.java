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
                unigram = "A" + itr.nextToken();
                termFrequency = itr.nextToken();
                context.write(new Text(docID), new Text(unigram + "\t" + termFrequency));
            }
        }
    }

    /**
     *
     * */
    public static class Job5MapperClass2 extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Split by sentences
            String[] sentences = value.toString().split("\\.");

            String docId = "";

            for (int i = 0; i < sentences.length; i++) {

                StringTokenizer itr = new StringTokenizer(sentences[i]);

                String sentence = "B";

                while (itr.hasMoreTokens()) {

                    String token = itr.nextToken();

                    if (token.contains("<====>")) {
                        docId = token.substring(token.indexOf("<====>") + 6, token.lastIndexOf("<====>"));
                        token = token.substring(token.lastIndexOf("<====>"));
                    }

                    sentence += token.replaceAll("<====>", "") + " ";

                }

                if(!sentence.equals("")) {
                    context.write(new Text(docId), new Text(sentence + "\t" + i));
                }
            }
        }

    }


    /**
     * Job 5 Reducer:
     * Input: (DocID, Iterable<(Sentence1), (word1,tfidf), (word2, tfldf), (Sentence2), ...>)
     * Select top 3 sentences with highest SentenceTF-IDF value for a given document
     * output: < DocumentID, {top three sentences} >
     */
    public static class Job5Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> sentences = new ArrayList<String>();
            HashMap<String, Double> word_tf_idf = new HashMap<String, Double>();

            // loop through the values, and grab all the sentences and {word/tfidf} values
            // put them into their corresponding collections
            for (Text val: values) {
                //it's a Unigram, TF_IDF pair
                if(val.toString().charAt(0) == 'A'){
                    String token = val.toString().substring(1);
                    String[] fields = token.split("\t");
                    //Hash Map: key: unigram, value: TF_IDF score
                    word_tf_idf.put(fields[0], Double.parseDouble(fields[1]));
                }
                else {
                    sentences.add(val.toString().substring(1));
                }
            }

            TreeMap<Double, String> top3 = new TreeMap<Double, String>();

            //loop through the sentences
            for(String s : sentences){

                TreeMap<Double, String> top5 = new TreeMap<Double, String>();
                // tokenize the words
                StringTokenizer itr = new StringTokenizer(s);

                while(itr.hasMoreTokens()){

                    String unigram = itr.nextToken();
                    //convert token to lowercase and get rid of all unwanted punctuation (not the -)
                    unigram = unigram.replaceAll("[^A-Za-z0-9]","").toLowerCase();

                    //if empty string then toss
                    if(unigram.equals("")) {
                        continue;
                    }

                    // get their TF_IDF value from the hash map
                    double TF_IDF = -1;
                    if(word_tf_idf.containsKey(unigram)){
                        TF_IDF = word_tf_idf.get(unigram);
                    }

                    //add to the tree if the unigram isn't already in the tree
                    if(!top5.values().contains(unigram)){
                        top5.put(TF_IDF, unigram);
                        //if the tree map is already greater than 5, then remove the smallest key
                        if(top5.size() > 5){
                            top5.remove(top5.firstKey());
                        }
                    }
                }

                //loop through the collection and sum them up to get the sentence TF-IDF
                Set<Double> top5values = top5.keySet();
                Double SentenceTF_IDF = 0.0;
                for(Double val : top5values){
                    SentenceTF_IDF += val;
                }

                //put the sentence into the top 3 tree map with it's SentenceTF_IDF
                top3.put(SentenceTF_IDF, s);
                if(top3.size() > 3){
                    top3.remove(top3.firstKey());
                }

            }

            //put these sentences into a tree map
            //Key: line number
            //Value: the sentence
            TreeMap<Double, String> sOrder = new TreeMap<Double, String>();
            for(String s : top3.values()) {
                String[] fields = s.split("\t");
                sOrder.put(Double.parseDouble(fields[1]), fields[0]);
            }

            //grab all the sentences and concat them
            Collection<String> orderedSentences = sOrder.values();
            String finalSentences = "";
            for(String s : orderedSentences){
                finalSentences += s + "\n\t";
            }
            context.write(key, new Text(finalSentences));

        }

    }

}