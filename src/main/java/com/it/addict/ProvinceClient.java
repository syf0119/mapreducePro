package com.it.addict;

import com.it.love.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ProvinceClient {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);


        job.setJarByClass(ProvinceClient.class);

        job.setMapperClass(ProvinceMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setReducerClass(ProvinceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(4);


        FileInputFormat.addInputPath(job,new Path("D://HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath(job,new Path("F://province"));


        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);


    }
}
