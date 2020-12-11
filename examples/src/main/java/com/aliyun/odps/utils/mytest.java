package com.aliyun.odps.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class mytest {

    public static void main(String[] args){
        String pattern = "(\\d+\\.?\\d+)\\s*x\\s*(\\d+\\.?\\d+)\\s*x\\s*(\\d+\\.?\\d+)\\s*(\\w+)";

//        String pattern = "([\\d|\\\\.]+)\\s*x\\s*([\\d|\\\\.]+)\\s*x\\s*([\\d|\\\\.]+)\\s*(\\w+)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher("7.2 x 8.3 x 2.1 yu");
        if (m.find() && m.group(1)!=null){
            System.out.println(m.group(1));
            System.out.println(m.group(2));
            System.out.println(m.group(3));
        }
//        StringBuilder result = new StringBuilder();
//        Map<String,Long> aa = new HashMap<>();
//        tools.split_string_dif(aa,"wefwefw ? in on of # . fs wefw");
//        System.out.println(aa);


    }
}
