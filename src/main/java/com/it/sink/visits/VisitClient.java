package com.it.sink.visits;

import com.it.sink.pageviews.PageViewsBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class VisitClient {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        File file=new File("F://visitview");
        while(file.exists()){
            deleteDir(file);
        }

        Configuration configuration=new Configuration();
        Job job=new Job(configuration);



        job.setMapperClass(VisitMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageViewsBean.class);


        job.setReducerClass(VisitReducer.class);
        job.setOutputKeyClass(VisitBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(VisitClient.class);


        FileInputFormat.addInputPath(job,new Path("F://pageview/part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("F://visitview"));


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
