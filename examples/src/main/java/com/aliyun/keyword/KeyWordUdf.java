package com.aliyun.keyword;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aliyun.odps.utils.tools.*;


@Resolve("STRING,ARRAY<STRUCT<KEYWORD:STRING,PAGE:BIGINT,LIST_ID:BIGINT,RANK_ID:BIGINT,ASIN:STRING," +
        "PRICE:DOUBLE,STARS:DOUBLE,REVIEWS_COUNT:BIGINT,PRODUCT_TITLE:STRING,IS_AD:BIGINT,SHIP:STRING," +
        "ALL_COUNT:BIGINT,ADD_DATE_TIME:DATETIME,NODE_ID:STRING,MONTH_SOLD_CNT:BIGINT>>->string")
public class KeyWordUdf extends UDF {
    public String evaluate(String keyword, List<Struct> all_item) {
        JSONObject result = new JSONObject();
        result.put("keyword",keyword);
        int all_item_cnt = all_item.size();
        long all_count=0;
        // 异常数据
        if (all_item_cnt ==0)
            return result.toJSONString();

        // 价格列表
        List<Double> price_list = new ArrayList<>();
        // 评论数列表
        List<Long> reviews_count_list = new ArrayList<>();
        // 星级列表
        List<Double> star_list = new ArrayList<>();
        // 样品总月销量
        long total_month_sold_cnt =0L;
        // 月销量列表
        ArrayList<Long>  price_range_month_sold_cnt_list = new ArrayList<>();
        // 价格列表
        ArrayList<Double>  price_range_price_list = new ArrayList<>();
        // 价格段对应的asin个数
        Map<String,Long> price_range_asin_cnt_map = new HashMap<>();
        // 价格段对应的月销量
        Map<String,Long> price_range_month_sold_cnt_map = new HashMap<>();
        // 星级段对应的asin个数
        Map<String,Long> stars_range_asin_cnt_map = new HashMap<>();
        // 星级段对应的月销量
        Map<String,Long> stars_range_month_sold_cnt_map = new HashMap<>();
        // 评分数段对应的asin个数
        Map<String,Long> reviews_cnt_range_asin_cnt_map = new HashMap<>();
        // 评分数段对应的月销量
        Map<String,Long> reviews_cnt_range_month_sold_cnt_map = new HashMap<>();
        // title 复杂分词
        Map<String,Long>  product_title_word_cnt = new HashMap<>();

        // amz 发货数量
        double amz_cnt = 0.0;
        // fba 发货数量
        double fba_cnt = 0.0;
        // fbm 发货数量
        double fbm_cnt = 0.0;
        for (Struct item : all_item) {
            all_count = (Long) item.getFieldValue("all_count");
            // 价格
            Double price = (Double) item.getFieldValue("price");
            if (price>0) price_list.add(price);
            // 评论数
            Long reviews_count = (Long) item.getFieldValue("reviews_count");
            String reviews_count_range = get_reviews_count_range(reviews_count);
            if (reviews_count>0) reviews_count_list.add(reviews_count);
            // 星级
            Double star = (Double) item.getFieldValue("stars");
            String stars_range = get_stars_range(star);
            if(star>0) star_list.add(star);
            // 物流
            String ship = (String) item.getFieldValue("ship");
            switch (ship) {
                case "AMZ":
                    amz_cnt += 1;
                    break;
                case "FBA":
                    fba_cnt += 1;
                    break;
                case "FBM":
                    fbm_cnt += 1;
                    break;
            }
            // 月销量
            Long month_sold_cnt = (Long) item.getFieldValue("month_sold_cnt");
            if (month_sold_cnt >0) {
                total_month_sold_cnt = total_month_sold_cnt + month_sold_cnt;
                map_key_add_value(stars_range_month_sold_cnt_map,stars_range,month_sold_cnt);
                map_key_add_value(reviews_cnt_range_month_sold_cnt_map,reviews_count_range,month_sold_cnt);
                if(price>0){
                    price_range_price_list.add(price);
                    price_range_month_sold_cnt_list.add(month_sold_cnt);
                }
            }
            // 各范围区间内的asin个数
            map_key_add_value(stars_range_asin_cnt_map,stars_range,1);
            map_key_add_value(reviews_cnt_range_asin_cnt_map,reviews_count_range,1);
            String product_title = (String) item.getFieldValue("product_title");
            split_string_dif(product_title_word_cnt,product_title);

        }
        // 商品总数
        result.put("keyword_all_count",all_count);
        // 取样产品数
        result.put("keyword_all_item_cnt",all_item_cnt);
        // 平均价格
        if (price_list.size()>0){
            Double keyword_avg_price = price_list.stream().reduce(Double::sum).get()/all_item_cnt;
            result.put("keyword_avg_price",get_double_acc(keyword_avg_price,2));
        }else{
            result.put("keyword_avg_price","");
        }
        // 最高价格
        result.put("keyword_max_price",price_list.stream().reduce(Double::max));
        // 平均评论数
        Double keyword_avg_review_count = reviews_count_list.stream().reduce(Long::sum).get()/ (double) all_item_cnt;
        result.put("keyword_avg_review_count",get_double_acc(keyword_avg_review_count,2));
        // 最大评论数
        result.put("keyword_max_review_count",reviews_count_list.stream().reduce(Long::max));
        // 平均星级
        Double keyword_avg_star = star_list.stream().reduce(Double::sum).get()/all_item_cnt;
        result.put("keyword_avg_star",get_double_acc(keyword_avg_star,2));
        // amz占比
        result.put("keyword_amz_rate",get_double_acc(amz_cnt/all_item_cnt,2));
        // fba占比
        result.put("keyword_fba_rate",get_double_acc(fba_cnt/all_item_cnt,2));
        // fbm占比
        result.put("keyword_fbm_rate",get_double_acc(fbm_cnt/all_item_cnt,2));
        // 价格分布
        if (price_range_price_list.size()>0) {
            double price_avg = list_double_avg(price_range_price_list);
            for (int i = 0; i < price_range_price_list.size(); i++) {
                Double price = price_range_price_list.get(i);
                String price_range = get_price_range(price, price_avg);
                Long month_sold = price_range_month_sold_cnt_list.get(i);
                map_key_add_value(price_range_asin_cnt_map, price_range, 1);
                if (month_sold != null && month_sold > 0) {
                    map_key_add_value(price_range_month_sold_cnt_map, price_range, month_sold);
                }
            }
            result.put("keyword_price_range",map_2_string_combine(price_range_month_sold_cnt_map,total_month_sold_cnt,price_range_asin_cnt_map));
        }else{
            result.put("keyword_price_range","");
        }
        // 星级分布
        result.put("keyword_stars_range",map_2_string_combine(stars_range_month_sold_cnt_map,total_month_sold_cnt,stars_range_asin_cnt_map));
        // 评分数分布
        result.put("keyword_reviews_cnt_range",map_2_string_combine(reviews_cnt_range_month_sold_cnt_map,total_month_sold_cnt,reviews_cnt_range_asin_cnt_map));
        // 标题词频分析
        result.put("keyword_title_frequency",sortMapByValues_string(product_title_word_cnt,1000));
        return result.toJSONString();
    }
}