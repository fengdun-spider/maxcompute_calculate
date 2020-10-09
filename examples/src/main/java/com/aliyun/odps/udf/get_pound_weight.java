package com.aliyun.odps.udf;

import com.aliyun.odps.udf.annotation.Resolve;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Resolve("String->double")
public class get_pound_weight extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public Double evaluate(String item_weight) {
        String pattern = "([\\d|\\\\.]+)\\s(\\w+)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(item_weight);
        // 美制单位
        if (m.find() && m.group(1)!=null && m.group(2)!=null ){
            switch (m.group(2)) {
                case "pounds":
                    return Double.parseDouble(m.group(1));
                case "ounces":
                    return Double.parseDouble(m.group(1)) / 16;
                case "drachms":
                    return Double.parseDouble(m.group(1)) / 256;
                case "stones":
                    return Double.parseDouble(m.group(1)) * 14;
                case "hundredweights":
                    return Double.parseDouble(m.group(1)) * 100;
                case "tons":
                    return Double.parseDouble(m.group(1)) * 2000;
            }
        }
        return null;
    }
    }