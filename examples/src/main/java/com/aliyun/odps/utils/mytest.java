package com.aliyun.odps.utils;

import java.util.HashMap;
import java.util.Map;

public class mytest {

    public static void main(String[] args){
        StringBuilder result = new StringBuilder();
        Map<String,Long> aa = new HashMap<>();
        tools.split_string_dif(aa,"wefwefw ? in on of # . fs wefw");
        System.out.println(aa);


    }
}
