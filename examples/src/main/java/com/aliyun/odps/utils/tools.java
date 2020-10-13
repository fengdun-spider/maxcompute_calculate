package com.aliyun.odps.utils;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;


public class tools {

    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    static Date now_date = new Date();

    //    求价格均值
    public static double list_double_avg(ArrayList<Double> price_list){
        double price_sum = 0;
        for (Double price : price_list){
            price_sum += price;
        }
        return price_sum/price_list.size();
    }

    //    判断日期是不是多少天内
    public static boolean date_is_new(String issue_date,int days){
        try{
            Date pub_date = sdf.parse(issue_date);
            double pub_days = (now_date.getTime()-pub_date.getTime())/(1000.0*3600*24);
            return pub_days <= days;
        } catch (Exception ex){
            return false;
        }
    }

    //   pt日期格式是否正确
    public static boolean valid_monday_pt(String pt){
        try{
            Date pt_date = sdf.parse(pt);
            Calendar c = Calendar.getInstance();
            c.setTime(pt_date);
            return pt_date.compareTo(now_date) < 0 && c.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY;
        }catch (Exception ex){
            return false;
        }
    }

    //    累加value更新map中key-value
    public static void map_key_add_value(Map<String,Long> map,String key,long value){
        if(map.containsKey(key)){
            map.replace(key,map.get(key)+value);
        }else{
            map.put(key,value);
        }
    }

    //    map按照value排序
    public static List<Map.Entry<String, Long>> sorted_entry_by_value(Map<String, Long> aMap) {
        HashMap<String, Long> finalOut = new LinkedHashMap<>();
        return aMap.entrySet()
                .stream()
                .sorted((p1, p2) -> p2.getValue().compareTo(p1.getValue()))
                .collect(Collectors.toList());
    }

    //  map按照value排序 ,并取指定数量键值对字符串,,,,
    public static String sortMapByValues_string_combine(Map<String, Long> map,int size,long month_sold_cnt,Map<String, Long> map2) {
        if(map.size()==0 || month_sold_cnt<=0){return "";}
        double month_sold_cnt_double = (double)  month_sold_cnt;
        StringBuilder result = new StringBuilder();
        List<Map.Entry<String, Long>> sort_entry_list = sorted_entry_by_value(map);
        for(int i=0, n=sort_entry_list.size();i<n;i++){
            double rate = sort_entry_list.get(i).getValue()/month_sold_cnt_double;
            Map.Entry<String, Long> entry = sort_entry_list.get(i);
            result.append(entry.getKey()).append(":").append(rate).append(":").append(map2.get(entry.getKey())).append("|||");
            if(i>=size){
                result.trimToSize();
                break;
            }
        }
        return result.substring(0, result.length() - 3).replace("null","");
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

    //  map变成string返回,,,,
    public static String map_2_string_combine(Map<String, Long> map,long month_sold_cnt,Map<String, Long> map2) {
        if(map.size()==0 ||month_sold_cnt<=0){return "";}
        double month_sold_cnt_double = (double)  month_sold_cnt;
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Long> entry : map.entrySet()){
            result.append(entry.getKey()).append(":").append(entry.getValue()/month_sold_cnt_double).append(":").append(map2.get(entry.getKey())).append("|||");
        }
        return result.substring(0, result.length() - 3).replace("null","");
    }

    // 价格分布
    public static String get_price_range(Double price,Double avg_price) {
        double unit = avg_price/5;
        double start = avg_price - unit*4;
        double end = 0;
        if (price<start){
            return "0-"+String.format("%.1f", start);
        }
        for(int i=0;i<=10;i++){
            end = start+unit;
            if (start<=price && price<end){
                return String.format("%.1f", start)+"-"+String.format("%.1f", end);
            }
            start = end;
        }
        if(price>=end){
            return String.format("%.1f", end)+"-"+"up";
        }
        return "";
    }

    // 星级分布
    public static String get_stars_range(Double stars) {
        if (stars == null){
            return "null";
        }else if(0<stars && stars<2){
            return "0-2";
        }else if(2<=stars && stars<2.5){
            return "2-2.5";
        }else if(2.5<=stars && stars<3){
            return "2.5-3";
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
    public static String get_reviews_count_range(Long reviews_count) {
        if(reviews_count==null){
            return "null";
        }else if(1<=reviews_count && reviews_count<100){
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



//    public static <K extends Comparable, V extends Comparable> Map<K, V> sortMapByValues(Map<K, V> aMap) {
//        HashMap<K, V> finalOut = new LinkedHashMap<>();
//        aMap.entrySet()
//                .stream()
//                .sorted((p1, p2) -> p2.getValue().compareTo(p1.getValue()))
//                .collect(Collectors.toList()).forEach(ele -> finalOut.put(ele.getKey(), ele.getValue()));
//        return finalOut;
