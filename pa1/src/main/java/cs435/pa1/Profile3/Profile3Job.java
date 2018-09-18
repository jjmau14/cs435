package cs435.pa1.Profile3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Profile3Job {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job preMap = Job.getInstance(conf, "PA1Profile3_1");
        preMap.setJarByClass(Profile3Job.class);
        preMap.setMapperClass(Profile3Maper.class);
        preMap.setReducerClass(Profile3Reducer.class);
        preMap.setNumReduceTasks(36);
        preMap.setPartitionerClass(Profile3Partitioner.class);
        preMap.setOutputKeyClass(Text.class);
        preMap.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(preMap, new Path(args[0]));
        FileOutputFormat.setOutputPath(preMap, new Path("profile3temp"));

        Job job = Job.getInstance(conf, "PA1Profile3_2");
        job.setJarByClass(Profile3Job.class);
        job.setMapperClass(Profile3Maper_2.class);
        job.setReducerClass(Profile3Reducer_2.class);
        job.setNumReduceTasks(30);
        job.setPartitionerClass(Profile3Partitioner_2.class);
        job.setSortComparatorClass(jobComparator.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("profile3temp"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (preMap.waitForCompletion(true))
            System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


