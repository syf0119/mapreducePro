package com.it.like;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


public class wcMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        String[] strs= value.toString().split(" ");
        System.out.println(Arrays.toString(strs));
        for (String str : strs) {
            System.out.println(str);
            context.write(new Text(str),new IntWritable(1));
        }
    }
}
