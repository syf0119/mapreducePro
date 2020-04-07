package com.it.sink.pageviews;

import com.it.sink.preprocess.WebLogBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class PageClient {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        File file=new File("F://pageview");
        while(file.exists()){
            deleteDir(file);
        }

        Configuration configuration=new Configuration();
        Job job=new Job(configuration);



        job.setMapperClass(PageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);


        job.setReducerClass(PageReducer.class);
        job.setOutputKeyClass(PageViewsBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(PageClient.class);

        FileInputFormat.addInputPath(job,new Path("F://out/part-m-00000"));
        FileOutputFormat.setOutputPath(job,new Path("F://pageview"));


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
