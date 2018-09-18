package cs435.pa1.Profile2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Profile2Job {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job preMap = Job.getInstance(conf, "PA1Profile2_1");
        preMap.setJarByClass(Profile2Job.class);
        preMap.setMapperClass(Profile2Mapper.class);
        preMap.setReducerClass(Profile2Reducer.class);
        preMap.setNumReduceTasks(30);
        preMap.setPartitionerClass(Profile2Partitioner.class);
        preMap.setOutputKeyClass(Text.class);
        preMap.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(preMap, new Path(args[0]));
        FileOutputFormat.setOutputPath(preMap, new Path("profile2temp"));


        Job job = Job.getInstance(conf, "PA1Profile2_1");
        job.setJarByClass(Profile2Job.class);
        job.setMapperClass(Profile2Mapper_2.class);
        job.setReducerClass(Profile2Reducer_2.class);
        job.setSortComparatorClass(Profile2Comparator.class);
        job.setNumReduceTasks(30);
        job.setPartitionerClass(Profile2Partitioner_2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("profile2temp"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        if (preMap.waitForCompletion(true))
            System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


