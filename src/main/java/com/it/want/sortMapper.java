package com.it.want;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class sortMapper extends Mapper<LongWritable,Text,LongWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split("\\t");
        context.write(new LongWritable(-Long.parseLong(strings[1].trim())),new Text(strings[0]));
    }
}
