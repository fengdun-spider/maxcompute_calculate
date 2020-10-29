package com.aliyun.keyword;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.ArrayList;
import java.util.List;

import static com.aliyun.odps.utils.tools.*;


@Resolve("STRING,BIGINT,ARRAY<STRUCT<keyword:STRING,page:BIGINT,list_id:BIGINT,asin:STRING," +
        "pic_url:STRING,price:DOUBLE,stars:DOUBLE,reviews_count:BIGINT,product_title:STRING," +
        "is_ad:BIGINT,ship:STRING,ship_fee:DOUBLE,add_date_time:DATETIME>>->string")
public class KeyWordUdf extends UDF {
    public String evaluate(String keyword, Long all_count,List<Struct> all_item) {
        JSONObject result = new JSONObject();
        int all_item_cnt = all_item.size();
        // 异常数据
        if (all_item_cnt ==0)
            return result.toJSONString();

        // 价格列表
        List<Double> price_list = new ArrayList<>();
        // 评论数列表
        List<Long> reviews_count_list = new ArrayList<>();
        // 星级列表
        List<Double> star_list = new ArrayList<>();
        // amz 发货数量
        double amz_cnt = 0.0;
        // fba 发货数量
        double fba_cnt = 0.0;
        // fbm 发货数量
        double fbm_cnt = 0.0;
        for (Struct item : all_item) {
            // 价格
            Double price = (Double) item.getFieldValue("price");
            if (price>0) price_list.add(price);
            // 评论数
            Long reviews_count = (Long) item.getFieldValue("reviews_count");
            if (reviews_count>0) reviews_count_list.add(reviews_count);
            // 星级
            Double star = (Double) item.getFieldValue("stars");
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
        }
        // 商品总数
        result.put("keyword_all_count",all_count);
        // 取样产品数
        result.put("keyword_all_item_cnt",all_item_cnt);
        // 平均价格
        Double keyword_avg_price = price_list.stream().reduce(Double::sum).get()/all_item_cnt;
        result.put("keyword_avg_price",get_double_acc(keyword_avg_price,2));
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
        return result.toJSONString();
    }
}