package com.aliyun.odps.utils;

import java.util.HashMap;
import java.util.Map;

public class mytest {

    public static void main(String[] args){
        StringBuilder result = new StringBuilder();
        Map<String,Integer> aa = new HashMap<>();
        String a;
        a = "111###2";
        System.out.println(a.split("###")[0]);
//        aa.put("a",2) ;
//        result.append("|||").append(aa.get("b")).append("222");
//        System.out.println(result);

    }
}
