package com.aliyun.odps.udf;

import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"bigint,bigint,bigint,double,double,double,bigint,bigint,double,double,bigint,double,double,double->bigint"})
public class get_market_profit_rate extends UDF {
    public Long evaluate(Long avg_sales, Long avg_reviews_count, Long avg_bsr, Double avg_price, Double avg_stars, Double avg_weight, Long top_100_brand_count, Long top_50_brand_count, Double product_concentration, Double brand_concentration, Long avg_sellers_count, Double fba_proportion, Double fbm_proportion, Double amz_proportion) {
        double result = 0.0;
        if(avg_sales==null||avg_sales<=0) result+=0;
        else if(avg_sales<=500) result+=0.7;
        else if(avg_sales<=1000) result+=1.2;
        else if(avg_sales<=3000) result+=1.8;
        else if(avg_sales<=6000) result+=2;
        else if(avg_sales<=10000) result+=1.8;
        else result+=1;

        if(avg_reviews_count==null || avg_reviews_count<=0) result+=0;
        else if(avg_reviews_count<=100) result+=0.4;
        else if(avg_reviews_count<=200) result+=0.6;
        else if(avg_reviews_count<=500) result+=0.8;
        else if(avg_reviews_count<=1000) result+=1;
        else if(avg_reviews_count<=2000) result+=1.5;
        else result+=1;

        if(avg_bsr==null||avg_bsr<=0) result+=0;
        else if(avg_bsr<=100) result+=0.4;
        else if(avg_bsr<=500) result+=0.8;
        else if(avg_bsr<=1000) result+=1;
        else if(avg_bsr<=3000) result+=1.2;
        else if(avg_bsr<=5000) result+=1.5;
        else result+=1;

        if(avg_price==null || avg_price<=0) result+=0;
        else if(avg_price<=10) result+=0.8;
        else if(avg_price<=20) result+=1.6;
        else if(avg_price<=30) result+=2;
        else if(avg_price<=50) result+=1.8;
        else if(avg_price<=100) result+=1.4;
        else result+=1;

        if(avg_stars==null || avg_stars<=0) result+=0;
        else if(avg_stars<=2) result+=0.4;
        else if(avg_stars<=3) result+=1;
        else if(avg_stars<=3.5) result+=1.5;
        else if(avg_stars<=4.1) result+=2;
        else if(avg_stars<=4.2) result+=1;
        else if(avg_stars<=4.5) result+=0.8;
        else result+=0.6;

        if(avg_weight==null || avg_weight<=0) result+=0;
        else if(avg_weight<=0.05) result+=0.4;
        else if(avg_weight<=0.1) result+=0.6;
        else if(avg_weight<=0.5) result+=1;
        else if(avg_weight<=1) result+=1.5;
        else if(avg_weight<=5) result+=0.6;
        else result+=0.4;

        if(top_100_brand_count==null || top_100_brand_count<=0) result+=0;
        else if(top_100_brand_count<=10) result+=0;
        else if(top_100_brand_count<=20) result+=0.6;
        else if(top_100_brand_count<=30) result+=0.8;
        else if(top_100_brand_count<=40) result+=1;
        else if(top_100_brand_count<=50) result+=1.4;
        else if(top_100_brand_count<=60) result+=1.6;
        else if(top_100_brand_count<=70) result+=2;
        else result+=1.8;

        if(top_50_brand_count==null||top_50_brand_count<=0) result+=0;
        else if(top_50_brand_count<=3) result+=0;
        else if(top_50_brand_count<=6) result+=0.3;
        else if(top_50_brand_count<=10) result+=0.4;
        else if(top_50_brand_count<=15) result+=0.5;
        else if(top_50_brand_count<=20) result+=0.6;
        else if(top_50_brand_count<=30) result+=0.8;
        else if(top_50_brand_count<=40) result+=1;
        else result+=0.9;

        if(product_concentration==null || product_concentration<0) result+= 0;
        else if(product_concentration<=0.05) result+=2;
        else if(product_concentration<=0.1) result+=1.6;
        else if(product_concentration<=0.15) result+=1.4;
        else if(product_concentration<=0.25) result+=1.2;
        else if(product_concentration<=0.45) result+=1;
        else if(product_concentration<=0.55) result+=0.8;
        else result+=0.6;

        if(brand_concentration==null || brand_concentration<0) result+=0;
        else if(brand_concentration<=0.5) result+=2;
        else if(brand_concentration<=0.1) result+=1.6;
        else if(brand_concentration<=0.15) result+=1.4;
        else if(brand_concentration<=0.25) result+=1.2;
        else if(brand_concentration<=0.35) result+=1;
        else if(brand_concentration<=0.45) result+=0.8;
        else result+=0.6;

        if(avg_sellers_count==null || avg_sellers_count<0) result+=0;
        else if(avg_sellers_count<=2) result+=0.4;
        else if(avg_sellers_count<=4) result+=0.6;
        else if(avg_sellers_count<=6) result+=0.8;
        else if(avg_sellers_count<=8) result+=1;
        else result+=0.8;

        if(fba_proportion==null || fba_proportion<0) result+=0;
        else if(fba_proportion<=0.05) result+=1;
        else if(fba_proportion<=0.1) result+=0.9;
        else if(fba_proportion<=0.15) result+=0.8;
        else if(fba_proportion<=0.25) result+=0.6;
        else if(fba_proportion<=0.45) result+=0.4;
        else if(fba_proportion<=0.55) result+=0.3;
        else result+=0.2;

        if(fbm_proportion==null || fbm_proportion<0) result+=0;
        else if(fbm_proportion<=0.05) result+=0.2;
        else if(fbm_proportion<=0.1) result+=0.3;
        else if(fbm_proportion<=0.15) result+=0.4;
        else if(fbm_proportion<=0.25) result+=0.6;
        else if(fbm_proportion<=0.45) result+=0.8;
        else if(fbm_proportion<=0.55) result+=0.9;
        else result+=1;

        if(amz_proportion==null || amz_proportion<0) result+=0;
        else if(amz_proportion<=0.05) result+=1;
        else if(amz_proportion<=0.1) result+=0.9;
        else if(amz_proportion<=0.15) result+=0.8;
        else if(amz_proportion<=0.25) result+=0.6;
        else if(amz_proportion<=0.45) result+=0.4;
        else if(amz_proportion<=0.55) result+=0.3;
        else result+=0.2;

        return (long) Math.ceil(result);
    }
}