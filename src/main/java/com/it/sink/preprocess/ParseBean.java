package com.it.sink.preprocess;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class ParseBean {
    private static WebLogBean webLogBean=new WebLogBean();
    private static StringBuffer sb=new StringBuffer();
    private static SimpleDateFormat sdfInStr=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
   private  static SimpleDateFormat sdfOutStr=new SimpleDateFormat("yyyy:MM:dd HH:mm:ss",Locale.US);
    public static  WebLogBean populateBean(String line)  {
        String[] tuple = line.split(" ");

        if (tuple.length < 12) {
            webLogBean.setValid(false);
            webLogBean.setRemote_addr(tuple[0]);
            webLogBean.setRemote_user(tuple[1] + " " + tuple[2]);
            webLogBean.setTime_local(parseDate(tuple[3].substring(1)));
            webLogBean.setStatus(tuple[6]);
            webLogBean.setBody_bytes_sent(tuple[7]);
            webLogBean.setHttp_referer(null);
            webLogBean.setHttp_user_agent(null);
            webLogBean.setRequest(null);
        }
        if (tuple.length == 12) {
            webLogBean.setValid(true);
            webLogBean.setRemote_addr(tuple[0]);
            webLogBean.setRemote_user(tuple[1] + " " + tuple[2]);
            webLogBean.setTime_local(parseDate(tuple[3].substring(1)));
            webLogBean.setStatus(tuple[8]);
            webLogBean.setBody_bytes_sent(tuple[9]);
            webLogBean.setHttp_referer(tuple[10]);
            webLogBean.setHttp_user_agent(tuple[11]);
            webLogBean.setRequest(tuple[5] + tuple[6] + tuple[7]);

        }
        if (tuple.length > 12) {
            webLogBean.setValid(true);
            webLogBean.setRemote_addr(tuple[0]);
            webLogBean.setRemote_user(tuple[1] + " " + tuple[2]);
            webLogBean.setTime_local(parseDate(tuple[3].substring(1)));
            webLogBean.setStatus(tuple[8]);
            webLogBean.setBody_bytes_sent(tuple[9]);
            webLogBean.setHttp_referer(tuple[10]);
            webLogBean.setRequest(tuple[5] + tuple[6] + tuple[7]);
            webLogBean.setHttp_user_agent(tuple[11]);
            sb = new StringBuffer();
            for (int x = 11; x < tuple.length; x++) {
                sb.append(tuple[x]);
            }

        }
        return webLogBean;
    }
    public static String parseDate(String inStr)  {
        Date parse = null;
        try {
            parse = sdfInStr.parse(inStr);
        } catch (ParseException e) {
            System.out.println(inStr);
        }
        String format = sdfOutStr.format(parse);
        return format;


    }

    public static SimpleDateFormat getSdf() {
        return sdfOutStr;
    }
}
