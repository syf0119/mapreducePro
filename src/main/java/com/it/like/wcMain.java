package com.it.like;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;


public class wcMain {
    static {
       // System.setProperty("user.name","syf");
    }
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(),"wc");
        job.setJarByClass(wcMain.class);

        job.setMapperClass(wcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setReducerClass(wcReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

       job.setNumReduceTasks(2);
       FileInputFormat.addInputPath(job,new Path("f:"+ File.separator+"word"));
        FileOutputFormat.setOutputPath(job,new Path("f://out"));
        boolean success = job.waitForCompletion(true);
        System.exit(success?0:1);
}

}
