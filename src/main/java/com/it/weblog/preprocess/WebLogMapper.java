package com.it.weblog.preprocess;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: Allen Woon
 */
public class WebLogMapper extends Mapper<LongWritable, Text,WebLogBean, NullWritable> {

    WebLogBean k = new WebLogBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split(" ");
        if(fields.length >11){
            k.setRemote_addr(fields[0]);
            k.setHttp_referer(fields[1]);
            k.setHttp_referer(fields[1]);
            k.setHttp_referer(fields[1]);
            k.setHttp_referer(fields[1]);
            k.setHttp_referer(fields[1]);
            k.setHttp_referer(fields[1]);
            k.setHttp_referer(fields[1]);
            k.setHttp_referer(fields[1]);
        }else {
            //不满足字段个数的脏数据 逻辑删除 非法的数据
            k.setValid(false);
        }
    }
}
