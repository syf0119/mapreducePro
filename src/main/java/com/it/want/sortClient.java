package com.it.want;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class sortClient {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(sortClient.class);

        job.setMapperClass(sortMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(sortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job,new Path("D://part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("F://sort"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
