package cs435.pa3;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;
import scala.Serializable;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public final class WikiBomb {

    public static class MapTitle implements Serializable {
        private String lineNumber;
        private String title;

        public MapTitle(String lineNumber, String title){
            this.lineNumber = lineNumber;
            this.title = title;
        }

        public String getLineNumber(){
            return lineNumber;
        }

        public String getTitle(){
            return title;
        }

        public void setLineNumber(String lineNumber){
            this.lineNumber = lineNumber;
        }

        public void setTitle(String title){
            this.title = title;
        }
    }

    public static void main(String[] args) throws Exception {

        SparkSession sc = SparkSession
                .builder()
                .appName("WikiBomb")
                .getOrCreate();

        // Title data set RDD
        JavaRDD<String> titleFile = sc.read().textFile(args[1]).javaRDD();
        //AtomicInteger lineNum = new AtomicInteger(0);

        //JavaRDD<MapTitle> titlesWithLineNumbers = titleFile.map(s -> {
        //    lineNum.addAndGet(1);
        //    return new MapTitle(lineNum.toString(), s);
        //});

        //Dataset<Row> titles = sc.createDataFrame(titlesWithLineNumbers, MapTitle.class);
        //titles.createOrReplaceTempView("titles");

        // Get a sub-graph containing the word “surfing”. (First filter out the links dataset)
        //Dataset<Row> titlesDs = sc.sql("SELECT * FROM titles WHERE UPPER(title) LIKE UPPER('%surfing%')");

        //JavaPairRDD<String, String> queriedTitles = titlesDs.javaRDD().mapToPair(row ->
        //        new Tuple2<>(row.getString(0), row.getString(1)));

        // Read link data set as RDD (Load data)
        JavaRDD<String> links_raw = sc.read().textFile(args[0]).javaRDD();

        // Map Key Values
        JavaPairRDD<String, String> links = links_raw.mapToPair(s -> {
            String[] keyValues = s.split(":");
            return new Tuple2<>(keyValues[0], keyValues[1].trim() + "4290745");
        });

        // Join in "Surfing"
        //JavaPairRDD<String, Tuple2<String,String>> surfing = queriedTitles.join(links);

        // Add Rocky Mountain National park
        //JavaPairRDD<String, String> wikibomb = surfing.mapToPair(s -> new Tuple2<>(s._1(), s._2()._2()));
        JavaPairRDD<String, String> wikibomb = links.mapToPair(s -> new Tuple2<>(s._1(), s._2()));
        JavaPairRDD<String, Double> ranks = wikibomb.mapValues(rs -> 1.0);

        for (int current = 0; current < 25; current++) {

            JavaPairRDD<String, Double> urlLink = links
                    .join(ranks)
                    .values()
                    .flatMapToPair(s -> {
                        String[] pageLinks = s._1().split("\\s");
                        int linkCount = pageLinks.length;
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : pageLinks) {
                            results.add(new Tuple2<>(n, s._2() / linkCount));
                        }
                        return results.iterator();
                    });

            ranks = urlLink.reduceByKey((a, b) -> a+b).mapValues(sum -> 0.15 + (sum * 0.85));
        }

        // Map <Title, Line #>
        AtomicInteger lineNumFinal = new AtomicInteger(0);
        JavaPairRDD<String, String> titlesWithLineNumbers2 = titleFile.mapToPair(s -> {
            lineNumFinal.addAndGet(1);
            return new Tuple2<>(lineNumFinal.toString(),s);
        });


        JavaRDD<Tuple2<String, Double>> PR_with_title = titlesWithLineNumbers2.join(ranks).values();


        // Switch page rank and title sort
        JavaPairRDD<Double, String> sort = PR_with_title.mapToPair(s ->
                new Tuple2<>(s._2(), s._1())).sortByKey(false);

        JavaPairRDD<String, Double> finalPageRank = sort.mapToPair(s -> new Tuple2<>(s._2(), s._1()));


        finalPageRank.saveAsTextFile(args[2]);
        sc.stop();

    }
}