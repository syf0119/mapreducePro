package com.it.want;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class sortReducer extends Reducer<LongWritable,Text,Text,LongWritable>{
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println(key.toString());
        for (Text value : values) {
        context.write(value,new LongWritable(-key.get()));
    }
}
}
