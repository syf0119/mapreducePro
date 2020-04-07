package com.it.sink.pageviews;

import com.it.sink.preprocess.WebLogBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageMapper extends Mapper<LongWritable,Text,Text,WebLogBean> {
    private WebLogBean webLogBean=new WebLogBean();
    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String[] tuple = line.toString().split("\001");
        if("true".equals(tuple[0])){
            Text text=new Text(tuple[1]);
            webLogBean.setRemote_addr(tuple[1]);
            webLogBean.setTime_local(tuple[3]);
            webLogBean.setRequest(tuple[4]);
            webLogBean.setStatus(tuple[5]);
            webLogBean.setBody_bytes_sent(tuple[6]);
            webLogBean.setHttp_referer(tuple[7]);
            webLogBean.setHttp_user_agent(tuple[8]);

            context.write(text,webLogBean);

        }
    }
}
