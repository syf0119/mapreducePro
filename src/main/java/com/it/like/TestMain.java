package com.it.like;

import java.util.Map;
import java.util.Properties;

public class TestMain {
    public static void main(String[] args) {
        Properties properties = System.getProperties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            System.out.println(entry.getKey()+"      "+entry.getValue());
        }

    }
}
