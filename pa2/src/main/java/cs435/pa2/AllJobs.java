package cs435.pa2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class AllJobs {

    public static class DocumentIdCounter {
        public enum numDocIds{
            N
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "pa2_job1");
        job1.setJarByClass(Job1.class);
        job1.setMapperClass(Job1.Job1Mapper.class);
        job1.setMapOutputKeyClass(CompositeKey.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setCombinerClass(Job1.Job1Combiner.class);
        job1.setNumReduceTasks(32);
        job1.setPartitionerClass(Job1.Job1Partitioner.class);
        job1.setReducerClass(Job1.Job1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        Job job2 = Job.getInstance(conf, "pa2_job2");
        job2.setJarByClass(Job2.class);
        job2.setMapperClass(Job2.Job2Mapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setNumReduceTasks(32);
        job2.setPartitionerClass(Job2.Job2Partitioner.class);
        job2.setReducerClass(Job2.Job2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        Job job3 = Job.getInstance(conf, "pa2_job3");
        job3.setJarByClass(Job3.class);
        job3.setMapperClass(Job3.Job3Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setNumReduceTasks(32);
        job3.setPartitionerClass(Job3.Job3Partitioner.class);
        job3.setReducerClass(Job3.Job3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        Job job4 = Job.getInstance(conf, "pa2_job4");
        job4.setJarByClass(Job4.class);
        job4.setMapperClass(Job4.Job4Mapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setNumReduceTasks(32);
        job4.setPartitionerClass(Job4.Job4Partitioner.class);
        job4.setReducerClass(Job4.Job4Reducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        Job job5 = Job.getInstance(conf, "pa2_job5");
        job5.setJarByClass(Job5.class);
        job5.setNumReduceTasks(32);
        job5.setPartitionerClass(Job5.Job5Partitioner.class);
        job5.setReducerClass(Job5.Job5Reducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_job1"));

        FileInputFormat.addInputPath(job2, new Path("temp_job1"));
        FileOutputFormat.setOutputPath(job2, new Path("temp_job2"));

        FileInputFormat.addInputPath(job3, new Path("temp_job2"));
        FileOutputFormat.setOutputPath(job3, new Path("temp_job3"));

        FileInputFormat.addInputPath(job4, new Path("temp_job3"));
        FileOutputFormat.setOutputPath(job4, new Path("temp_job4"));

        MultipleInputs.addInputPath(job5, new Path("temp_job4"), TextInputFormat.class, Job5.Job5MapperClass1.class);
        MultipleInputs.addInputPath(job5, new Path(args[1]), TextInputFormat.class, Job5.Job5MapperClass2.class);

        FileOutputFormat.setOutputPath(job5, new Path(args[2]));

        if(job1.waitForCompletion(true)){
            if(job2.waitForCompletion(true)){
                Counter count = job2.getCounters().findCounter(DocumentIdCounter.numDocIds.N);
                if(job3.waitForCompletion(true)){
                    job4.getConfiguration().setLong(DocumentIdCounter.numDocIds.N.name(), count.getValue());
                    if(job4.waitForCompletion(true)){
                        System.exit(job5.waitForCompletion(true) ? 0 : 1);
                    }
                }
            }
        }
    }

}