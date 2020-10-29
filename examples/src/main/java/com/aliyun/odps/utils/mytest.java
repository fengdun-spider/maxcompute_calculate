package com.aliyun.odps.utils;

import java.util.HashMap;
import java.util.Map;

public class mytest {

    public static void main(String[] args){
        StringBuilder result = new StringBuilder();
        Map<String,Long> aa = new HashMap<>();
        aa.put("a",1L);
        aa.put("b",5L);
        aa.put("c",2L);
        aa.put("e",3L);

        System.out.println(tools.get_double_acc(1.2222,2));


    }
}
