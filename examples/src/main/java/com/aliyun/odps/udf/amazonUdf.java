package com.aliyun.odps.udf;


import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.*;

import static com.aliyun.odps.utils.tools.date_is_new;
import static com.aliyun.odps.utils.tools.map_key_add_value;


@Resolve("ARRAY<STRUCT<rank:BIGINT,week_sold_cnt:BIGINT,week_sold_money:DOUBLE,month_sold_cnt:BIGINT," +
        "month_sold_money:DOUBLE,product_dimensions:DOUBLE,bsr1:BIGINT,price:DOUBLE," +
        "reviews_count:BIGINT,stars:DOUBLE,offer_listing:BIGINT,ship:STRING,asin:STRING," +
        "brand:STRING,soldby:STRING,shipping_weight:STRING,ask_count:BIGINT," +
        "item_weight:DOUBLE,node_id:STRING,product_node:STRING,bsr1path:STRING,issue_date:STRING>>->string")
public class amazonUdf extends UDF {

    public String evaluate(List<Struct> all_item) {
//        样本个数
        double all_item_cnt = all_item.size();
//        月总销量
        long all_item_month_sold_cnt_sum = 0;
//        有月销量的item个数
        double all_item_month_sold_cnt_num = 0;
//        月总销售额
        double all_item_month_sold_money_sum = 0;
//        有月销售额的item个数
        double all_item_month_sold_money_num =0;
//        周总销量
        long all_item_week_sold_cnt_sum = 0;
//        有周销量的item个数
        double all_item_week_sold_cnt_num = 0;
//        周总销售额
        double all_item_week_sold_money_sum = 0;
//        有周销售额的item个数
        double all_item_week_sold_money_num = 0;
//        所有样品的bsr1的总和
        long all_item_bsr1_sum = 0;
//        所有bsr1排行的样品个数
        double all_item_bsr1_num = 0;
//        所有样品的价格总和
        double all_item_price_sum = 0;
//        所有样品有价格的个数
        double all_item_price_num = 0;
//        所有样品的评分总数
        long all_item_reviews_cnt_sum = 0;
//        所有样品有评分的个数
        double all_item_reviews_cnt_num = 0;
//        top10所有样品评分总数
        double top10_item_reviews_cnt_sum = 0;
//        所有样品星级之和
        double all_item_stars_sum =0;
//        所有样品有星级的个数
        double all_item_stars_num = 0;
//        所有样品的重量和
        double all_item_weight_sum = 0;
//        所有样品有重量的个数
        double all_item_weight_num = 0;
//        所有商品的体积和
        double all_item_dimensions_sum = 0;
//        所有商品有体积的个数
        double all_item_dimensions_num = 0;
//        top10商品的bsr1总和
        long top10_item_bsr1_sum = 0;
//        top10有bsr1的商品个数
        double top10_item_bsr1_num =0;
//        top10商品的月销量总和
        long top10_item_month_sold_cnt_sum = 0;
//        top10商品的月销量个数
        double top10_item_month_sold_cnt_num =0;
//        top10商品的月总销售额
        double top10_all_item_month_sold_money_sum = 0;
//        top10商品的有月销售额的item个数
        double top10_all_item_month_sold_money_num =0;

//        不同品牌集合
        Set<String> all_item_brand_set = new HashSet<>();
//        top50不同品牌集合
        Set<String> top50_item_brand_set = new HashSet<>();
//        品牌销量的map
        Map<String,Long> brand_sold_cnt_map = new HashMap<>();
//        所有商品的跟卖数量
        long all_item_offer_listing_cnt_sum = 0;
//        商品fba个数
        long all_item_fba_cnt = 0;
//        商品fbm个数
        long all_item_fbm_cnt = 0;
//        商品amz个数
        long all_item_amz_cnt = 0;

//        样品中的新品个数
        long new_item_cnt = 0;
//        样品中的新品评论总数
        long new_item_reviews_cnt_sum = 0;
//        样品中的价格总数
        long new_item_price_sum = 0;
//        样品中新品星级总数
        long new_item_stars_sum = 0;
//        样品中新品月销量总数
        long new_item_price_sold_cnt_sum = 0;
//        样品中新品月销量额总数
        long new_item_sold_money_sum = 0;

        for (Struct item : all_item) {
            Long month_sold_cnt = (Long) item.getFieldValue("month_sold_cnt");
            Double month_sold_money = (Double) item.getFieldValue("month_sold_money");
            Long week_sold_cnt = (Long) item.getFieldValue("week_sold_cnt");
            Double week_sold_money = (Double) item.getFieldValue("week_sold_money");
            Long reviews_count = (Long) item.getFieldValue("reviews_count");
            Long bsr1 = (Long) item.getFieldValue("bsr1");
            Double price = (Double) item.getFieldValue("price");
            Double stars = (Double) item.getFieldValue("stars");
            Double item_weight_double = (Double) item.getFieldValue("item_weight");
            Double product_dimensions_double = (Double) item.getFieldValue("product_dimensions");
            Long rank = (Long) item.getFieldValue("rank");
            String brand = (String) item.getFieldValue("brand");
            Long offer_listing = (Long) item.getFieldValue("offer_listing");
            String ship = (String) item.getFieldValue("ship");
            String issue_date = (String) item.getFieldValue("issue_date");
            boolean item_is_new = date_is_new(issue_date,90);

            if (month_sold_cnt > 0) {
                all_item_month_sold_cnt_sum += month_sold_cnt;
                all_item_month_sold_cnt_num += 1;
                if (rank <= 10) {
                    top10_item_month_sold_cnt_sum += month_sold_cnt;
                    top10_item_month_sold_cnt_num += 1;
                }
            }
            if (month_sold_money > 0) {
                all_item_month_sold_money_sum += month_sold_money;
                all_item_month_sold_money_num += 1;
                if (rank <= 10) {
                    top10_all_item_month_sold_money_sum += month_sold_money;
                    top10_all_item_month_sold_money_num += 1;
                }
            }
            if (week_sold_cnt > 0) {
                all_item_week_sold_cnt_sum += week_sold_cnt;
                all_item_week_sold_cnt_num += 1;
            }
            if (week_sold_money > 0) {
                all_item_week_sold_money_sum += week_sold_money;
                all_item_week_sold_money_num += 1;
            }
            if(reviews_count > 0){
                all_item_reviews_cnt_sum += reviews_count;
                all_item_reviews_cnt_num += 1;
                if(rank <= 10){
                    top10_item_reviews_cnt_sum += reviews_count;
                }
            }
            if (bsr1 > 0) {
                all_item_bsr1_sum += bsr1;
                all_item_bsr1_num += 1;
                if (rank <= 10) {
                    top10_item_bsr1_sum += bsr1;
                    top10_item_bsr1_num += 1;
                }
            }
            if (price > 0) {
                all_item_price_sum += price;
                all_item_price_num += 1;
            }
            if (stars > 0) {
                all_item_stars_sum += stars;
                all_item_stars_num += 1;
            }
            if (item_weight_double > 0) {
                all_item_weight_sum += item_weight_double;
                all_item_weight_num += 1;
            }
            if (product_dimensions_double > 0) {
                all_item_dimensions_sum += product_dimensions_double;
                all_item_dimensions_num += 1;
            }
            if (!brand.isEmpty()){
                all_item_brand_set.add(brand);
                if(rank<=50){
                    top50_item_brand_set.add(brand);
                }
                map_key_add_value(brand_sold_cnt_map,brand,reviews_count<0?0:reviews_count);
            }
            if(offer_listing>0){
                all_item_offer_listing_cnt_sum += offer_listing;
            }
            switch (ship) {
                case "AMZ":
                    all_item_amz_cnt += 1;
                    break;
                case "FBA":
                    all_item_fba_cnt += 1;
                    break;
                case "FBM":
                    all_item_fbm_cnt += 1;
                    break;
            }
            if (item_is_new){
                new_item_cnt+=1;
                if(reviews_count>0){
                    new_item_reviews_cnt_sum += reviews_count;
                }
                if(price>0){
                    new_item_price_sum += price;
                }
                if(stars>0){
                    new_item_stars_sum += stars;
                }
                if(month_sold_cnt>0){
                    new_item_price_sold_cnt_sum += month_sold_cnt;
                }
                if(month_sold_money>0){
                    new_item_sold_money_sum += month_sold_money;
                }
            }
        }

        JSONObject result = new JSONObject();
//        样本数
        result.put("all_item_cnt",all_item_cnt);
//        月均销量
        Double month_sold_avg = all_item_month_sold_cnt_num==0?null:all_item_month_sold_cnt_sum/all_item_month_sold_cnt_num;
        result.put("month_sold_avg",month_sold_avg);
//        月均销售额
        Double month_sold_money_avg = all_item_month_sold_money_num==0?null:all_item_month_sold_money_sum/all_item_month_sold_money_num;
        result.put("month_sold_money_avg",month_sold_money_avg);
//        周均销量
        Double week_sold_avg = all_item_week_sold_cnt_num==0?null:all_item_week_sold_cnt_sum/all_item_week_sold_cnt_num;
        result.put("week_sold_avg",week_sold_avg);
//        周均销售额
        Double week_sold_money_avg = all_item_week_sold_money_num==0?null:all_item_week_sold_money_sum/all_item_week_sold_money_num;
        result.put("week_sold_money_avg",week_sold_money_avg);
//        平均评论数
        Double reviews_count_avg =all_item_reviews_cnt_num==0?null:all_item_reviews_cnt_sum/all_item_reviews_cnt_num;
        result.put("reviews_count_avg",reviews_count_avg);
//        平均BSR
        Double bsr1_avg = all_item_bsr1_num==0?null:all_item_bsr1_sum/all_item_bsr1_num;
        result.put("bsr1_avg",bsr1_avg);
//        平均价格
        Double price_avg = all_item_price_num==0?null:all_item_price_sum/all_item_price_num;
        result.put("price_avg",price_avg);
//        平均星级
        Double stars_avg = all_item_stars_num==0?null:all_item_stars_sum/all_item_stars_num;
        result.put("stars_avg",stars_avg);
//        平均重量
        Double weight_avg = all_item_weight_num==0?null:all_item_weight_sum/all_item_weight_num;
        result.put("weight_avg",weight_avg);
//        平均体积
        Double dimensions_avg = all_item_dimensions_num==0?null:all_item_dimensions_sum/all_item_dimensions_num;
        result.put("dimensions_avg",dimensions_avg);
//        头部Listing平均BSR
        Double top10_bsr1_avg = top10_item_bsr1_num==0?null:top10_item_bsr1_sum/top10_item_bsr1_num;
        result.put("top10_bsr1_avg",top10_bsr1_avg);
//        头部Listing月均销量
        Double top10_month_sold_avg = top10_item_month_sold_cnt_num==0?null:top10_item_month_sold_cnt_sum/top10_item_month_sold_cnt_num;
        result.put("top10_month_sold_avg",top10_month_sold_avg);
//        头部Listing月均销售额
        Double top10_month_sold_money_avg = top10_all_item_month_sold_money_num==0?null:top10_all_item_month_sold_money_sum/top10_all_item_month_sold_money_num;
        result.put("top10_month_sold_money_avg",top10_month_sold_money_avg);
//        前100item品牌数量
        result.put("brand_cnt",all_item_brand_set.size());
//        前50item品牌数量
        result.put("top50_brand_cnt",top50_item_brand_set.size());
//        商品集中度
        Double top10_item_sold_money_rate = top10_item_reviews_cnt_sum<=0 || all_item_reviews_cnt_sum<=0? null:top10_item_reviews_cnt_sum/all_item_reviews_cnt_sum;
        result.put("top10_item_sold_money_rate",top10_item_sold_money_rate);
//        品牌集中度
        Long[] brand_sold_cnt_list = brand_sold_cnt_map.values().stream().sorted(Comparator.reverseOrder()).toArray(Long[]::new);
        double top10_brand_review_cnt = 0;
        for(int i=0, n=brand_sold_cnt_list.length;i<n;i++){
            top10_brand_review_cnt +=  brand_sold_cnt_list[i];
            if(i>=10){ break; }
        }
        Double top10_brand_sold_cnt_rate = top10_brand_review_cnt<=0 || all_item_reviews_cnt_sum<=0? null:top10_brand_review_cnt/all_item_reviews_cnt_sum;
        result.put("top10_brand_sold_cnt_rate",top10_brand_sold_cnt_rate);
//        平均卖家数
        double seller_cnt_avg = (all_item_offer_listing_cnt_sum + all_item_cnt)/all_item_cnt;
        result.put("seller_cnt_avg",seller_cnt_avg);
//        FBA占比
        result.put("fba_rate",all_item_fba_cnt/all_item_cnt);
//        FBM占比
        result.put("fbm_rate",all_item_fbm_cnt/all_item_cnt);
//        AMZ占比
        result.put("amz_rate",all_item_amz_cnt/all_item_cnt);
//        新品数量占比
        result.put("new_item_cnt_rate",new_item_cnt/all_item_cnt);
//        新品平均评论数
        result.put("new_item_reviews_cnt_sum_avg",new_item_cnt<=0?null:new_item_reviews_cnt_sum/new_item_cnt);
//        新品平均价格
        result.put("new_item_price_sum_avg",new_item_cnt<=0?null:new_item_price_sum/new_item_cnt);
//        新品平均星级
        result.put("new_item_stars_sum_avg",new_item_cnt<=0?null:new_item_stars_sum/new_item_cnt);
//        新品月均销量
        result.put("new_item_price_sold_cnt_sum_avg",new_item_cnt<=0?null:new_item_price_sold_cnt_sum/new_item_cnt);
//        新品月均销售额
        result.put("new_item_sold_money_sum_avg",new_item_cnt<=0?null:new_item_sold_money_sum/new_item_cnt);
        return result.toJSONString();
    }
}