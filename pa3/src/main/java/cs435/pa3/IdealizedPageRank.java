package cs435.pa3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public final class IdealizedPageRank {

    public static void main(String[] args) throws Exception {

        SparkSession sc = SparkSession
                .builder()
                .appName("IdealizedPageRank")
                .getOrCreate();

        // Link data set RDD
        JavaRDD<String> linkDataSet = sc.read().textFile(args[0]).javaRDD();

        // New RDD, map link data as key value
        JavaPairRDD<String, String> links = linkDataSet.mapToPair(s -> {
            String[] keyValues = s.split(":");
            return new Tuple2<>(keyValues[0], keyValues[1].trim());
        }).distinct().cache();

        // Default ranks 1.0
        JavaPairRDD<String, Double> pageRanks = links.mapValues(rs -> 1.0);

        // 25 iterations of std page rank
        for (int i = 0 ; i < 25 ; i++) {

            JavaPairRDD<String, Double> urlLink = links
                    .join(pageRanks)
                    .values()
                    .flatMapToPair(kv -> {
                        String[] pageLinks = kv._1().split("\\s");
                        List<Tuple2<String, Double>> newPageRanks = new ArrayList<>();
                        for (String n : pageLinks) {
                            newPageRanks.add(new Tuple2<>(n, kv._2() / pageLinks.length));
                        }
                        return newPageRanks.iterator();
                    });

            pageRanks = urlLink.reduceByKey((a, b) -> a+b).mapValues(sum -> sum);
        }

        // Title data set RDD
        JavaRDD<String> titleDataSet = sc.read().textFile(args[1]).javaRDD();
        AtomicInteger lineNum = new AtomicInteger(0);

        // <Title, Line #>
        JavaPairRDD<String, String> titles = titleDataSet.mapToPair(s -> {
            lineNum.addAndGet(1);
            return new Tuple2<>(lineNum.toString(),s);
        });

        // Inner Join Titles and PageRanks
        // <Title, PageRank>
        JavaRDD<Tuple2<String, Double>> pageRankTitle = titles.join(pageRanks).values();

        // Swap key value and sort by page rank desc
        JavaPairRDD<Double, String> sorted = pageRankTitle.mapToPair(s ->
                new Tuple2<>(s._2(), s._1())).sortByKey(false);

        // Unswap
        JavaPairRDD<String, Double> finalPageRank = sorted.mapToPair(s -> new Tuple2<>(s._2(), s._1()));

        finalPageRank.saveAsTextFile(args[2]);
        sc.stop();

    }
}