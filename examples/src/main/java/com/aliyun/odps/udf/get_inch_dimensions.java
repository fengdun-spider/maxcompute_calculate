package com.aliyun.odps.udf;

import com.aliyun.odps.udf.annotation.Resolve;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Resolve("String->DOUBLE")
public class get_inch_dimensions extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public Double evaluate(String dimensions) {
        String pattern = "(\\d+\\.?\\d+)\\s*x\\s*(\\d+\\.?\\d+)\\s*x\\s*(\\d+\\.?\\d+)\\s*(\\w+)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(dimensions);
        // 美制单位
        if (m.find() && m.group(4)!=null && m.group(3)!=null ){
            if (m.group(4).equals("inches")){
                return Double.parseDouble(m.group(1))*Double.parseDouble(m.group(2))*Double.parseDouble(m.group(3));
            }else if(m.group(4).equals("feet")){
                return Double.parseDouble(m.group(1))*Double.parseDouble(m.group(2))*Double.parseDouble(m.group(3)) *1728;
            }
        }
        return null;
    }
    }