package com.it.fall;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class OrderReducer extends Reducer<Text,OrderBean,OrderBean,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        List<OrderBean> list=new ArrayList();

        for (OrderBean value : values) {
        OrderBean orderBean=new OrderBean();
            try {
                BeanUtils.copyProperties(orderBean,value);
                list.add(orderBean);

            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
        Collections.sort(list,new OrderComparator());
        context.write(list.get(0),NullWritable.get());
        if(list.size()>1){
            context.write(list.get(1),NullWritable.get());
        }
    }
}
class OrderComparator implements Comparator<OrderBean>{

    @Override
    public int compare(OrderBean o1, OrderBean o2) {
        return (int) (o2.getMoney()-o1.getMoney());
    }
}
