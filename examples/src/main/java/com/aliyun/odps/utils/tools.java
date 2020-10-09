package com.aliyun.odps.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class tools {

    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    static Date now_date = new Date();

    //  long 数值型 cast null 为-1
    public static long cast_null_negative_long(Object source){
        if(source==null){
            return -1;
        }else {
            return (long) source;
        }
    }

    //  double 数值型 cast null 为-1
    public static double cast_null_negative_double(Object source){
        if(source==null){
            return -1;
        }else {
            return (double) source;
        }
    }

    //    重量转换函数
    public static double get_pound_weight(String item_weight){
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
        return -1;
    }

    //    体积转换函数
    public static double get_inch_dimensions(String dimensions){
        String pattern = "([\\d|\\\\.]+)\\s*x\\s*([\\d|\\\\.]+)\\s*x\\s*([\\d|\\\\.]+)\\s*(\\w+)";
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
        return -1;
    }

    //    品牌字符串处理函数
    public static String handle_brand_string(String brand){
        return brand.replace("Visit the ","").replace(" Store","").replace("Brand: ","");
    }

    //    判断日期是不是多少天内
    public static boolean date_is_new(String issue_date,int days) throws ParseException {
        Date pub_date = sdf.parse(issue_date);
        double pub_days = (now_date.getTime()-pub_date.getTime())/(1000.0*3600*24);
        return pub_days <= days;
    }

    //    更新map中key，value
    public static void map_key_add_value(Map<String,Long> map,String key,long value){
        if(map.containsKey(key)){
            map.replace(key,map.get(key)+value);
        }else{
            map.put(key,value);
        }
    }
//    public static <K extends Comparable, V extends Comparable> Map<K, V> sortMapByValues(Map<K, V> aMap) {
//        HashMap<K, V> finalOut = new LinkedHashMap<>();
//        aMap.entrySet()
//                .stream()
//                .sorted((p1, p2) -> p2.getValue().compareTo(p1.getValue()))
//                .collect(Collectors.toList()).forEach(ele -> finalOut.put(ele.getKey(), ele.getValue()));
//        return finalOut;

    //    map按照value排序
    public static List<Map.Entry<String, Long>> sorted_entry_by_value(Map<String, Long> aMap) {
        HashMap<String, Long> finalOut = new LinkedHashMap<>();
        return aMap.entrySet()
                .stream()
                .sorted((p1, p2) -> p2.getValue().compareTo(p1.getValue()))
                .collect(Collectors.toList());
    }

    //    map按照value排序 ,并取指定数量键值对字符串
    public static String sortMapByValues_string(Map<String, Long> map,int size) {
        System.out.println(map);
        if(map.size()==0){return "";}
        StringBuilder result = new StringBuilder();
        List<Map.Entry<String, Long>> sort_entry_list = sorted_entry_by_value(map);
        for(int i=0, n=sort_entry_list.size();i<n;i++){
            result.append(sort_entry_list.get(i).getKey()).append(":").append(sort_entry_list.get(i).getValue().toString()).append("|||");
            if(i>=size){
                result.trimToSize();
                break;
            }
        }
        return result.substring(0, result.length() - 3);
    }

    //    map按照value排序 ,并取指定数量键值对字符串
    public static String sortMapByValues_string(Map<String, Long> map,int size,long month_sold_cnt) {
        if(map.size()==0 || month_sold_cnt==0){return "";}
        double month_sold_cnt_double = (double)  month_sold_cnt;
        StringBuilder result = new StringBuilder();
        List<Map.Entry<String, Long>> sort_entry_list = sorted_entry_by_value(map);
        for(int i=0, n=sort_entry_list.size();i<n;i++){
            double rate = sort_entry_list.get(i).getValue()/month_sold_cnt_double;
            result.append(sort_entry_list.get(i).getKey()).append(":").append(rate).append("|||");
            if(i>=size){
                result.trimToSize();
                break;
            }
        }
        return result.substring(0, result.length() - 3);
    }

    //  map变成string返回
    public static String map_2_string(Map<String, Long> map) {
        if(map.size()==0){return "";}
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Long> entry : map.entrySet()){
            result.append(entry.getKey()).append(":").append(entry.getValue()).append("|||");
        }
        return result.substring(0, result.length() - 3);
    }

    // 价格分布
    public static String get_price_range(double price) {
        if (0<=price && price<40) {
            return "0-40";
        }else if (40<=price && price<80) {
            return "40-80";
        } else if (80<=price && price<120) {
            return "80-120";
        }else if (120<=price && price<160) {
            return "120-160";
        }else if (160<=price && price<200) {
            return "160-200";
        }else if (200<=price && price<240) {
            return "200-240";
        }else if (240<=price && price<280) {
            return "240-280";
        }else if (280<=price && price<320) {
            return "280-320";
        }else if (320<=price && price<360) {
            return "320-360";
        }else if(360<=price){
            return "360up";
        }else{
            return "null";
        }
    }

    // 星级分布
    public static String get_stars_range(double stars) {
        if(0<stars && stars<2){
            return "0-2";
        }else if(2<=stars && stars<3){
            return "2-3";
        }else if(3<=stars && stars<3.5){
            return "3-3.5";
        }else if(3.5<=stars && stars<4){
            return "3.5-4";
        }else if(4<=stars && stars<4.5){
            return "4-4.5";
        }else if(4.5<=stars && stars<=5){
            return "4.5-5";
        }else{
            return "null";
        }
    }

    // 评分数分布
    public static String get_reviews_count_range(long reviews_count) {
        if(1<=reviews_count && reviews_count<100){
            return "1-100";
        }else if(100<=reviews_count && reviews_count<200){
            return "100-200";
        }else if(200<=reviews_count && reviews_count<300){
            return "200-300";
        }else if(300<=reviews_count && reviews_count<500){
            return "300-500";
        }else if(500<=reviews_count && reviews_count<1000){
            return "500-1000";
        }else if(100<=reviews_count){
            return "1000up";
        }else {
            return "null";
        }
    }
}
