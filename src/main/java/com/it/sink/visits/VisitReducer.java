package com.it.sink.visits;

import com.it.sink.pageviews.PageViewsBean;
import com.it.sink.pageviews.PageViewsBeanComparator;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VisitReducer extends Reducer<Text,PageViewsBean,VisitBean,NullWritable> {
    VisitBean visitBean=new VisitBean();
    @Override
    protected void reduce(Text key, Iterable<PageViewsBean> values, Context context) throws IOException, InterruptedException {

        List<PageViewsBean> list=new ArrayList<>();

        for (PageViewsBean value : values) {
           PageViewsBean pageViewsBean=new PageViewsBean();
            try {
                BeanUtils.copyProperties(pageViewsBean,value);
                list.add(pageViewsBean);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
        //        private String session;
//        private String remote_addr;
//        private String inTime;
//        private String outTime;
//        private String inPage;
//        private String outPage;
//        private String referal;
//        private int pageVisits;
        Collections.sort(list,new  PageViewsBeanComparator());
        visitBean.setSession(key.toString());
        visitBean.setRemote_addr(list.get(0).getRemote_addr());
        visitBean.setInTime(list.get(0).getTimestr());
        visitBean.setOutTime(list.get(list.size()-1).getTimestr());
        visitBean.setInPage(list.get(0).getRequest());
        visitBean.setOutPage(list.get(list.size()-1).getRequest());
        visitBean.setReferal(list.get(0).getReferal());
        visitBean.setPageVisits(list.size());
        context.write(visitBean,NullWritable.get());

    }
}
