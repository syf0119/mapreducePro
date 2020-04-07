package com.it.sink.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class WebLogClient  {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


            File file=new File("F://out");
            while(file.exists()){
                deleteDir(file);
            }

        Configuration configuration=new Configuration();
        Job job=new Job(configuration);



        job.setMapperClass(WebLogMapper.class);
        job.setMapOutputKeyClass(WebLogBean.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setNumReduceTasks(0);


        job.setJarByClass(WebLogClient.class);


        FileInputFormat.addInputPath(job,new Path("F://access.log.20181101.dat"));
        FileOutputFormat.setOutputPath(job,new Path("F://out"));
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
    public static void deleteDir(File file){
        if(!file.delete()){
            File[] files = file.listFiles();
            for (File file1 : files) {
                deleteDir(file1);
            }
        }
    }
}
