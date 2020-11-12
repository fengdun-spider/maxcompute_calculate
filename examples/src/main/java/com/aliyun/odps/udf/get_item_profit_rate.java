package com.aliyun.odps.udf;

import com.aliyun.odps.udf.annotation.Resolve;

import static com.aliyun.odps.utils.tools.get_pub_days;

@Resolve({"bigint,string,string,bigint,double,bigint,double,bigint,string,bigint->bigint"})
public class get_item_profit_rate extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public Long evaluate(Long month_sold_cnt,String product_size,String issue_date,Long bsr1,Double price,Long reviews_count,Double stars, Long offer_listing, String ship,Long is_best_seller) {
        double result = 0.0;

        if(month_sold_cnt==null||month_sold_cnt<=0) result +=0;
        else if(month_sold_cnt<=50) result +=0.3;
        else if(month_sold_cnt<=100) result +=0.5;
        else if(month_sold_cnt<=300) result += 0.8;
        else if(month_sold_cnt<=600) result +=1;
        else if(month_sold_cnt<=1000) result+=0.8;
        else result +=0.6;

        if (product_size==null || product_size.isEmpty()) result +=0.5;
        else if(product_size.equals("小号标准尺寸")) result +=1;
        else if(product_size.equals("大号标准尺寸")) result+=0.8;
        else if(product_size.equals("小号大件")) result += 0.8;
        else if(product_size.equals("中号大件")) result += 0.6;
        else if(product_size.equals("大号大件")) result += 0.3;
        else if(product_size.equals("特殊大件")) result += 0;
        else if(product_size.equals("其它尺寸")) result += 0.5;

        int pub_days = get_pub_days(issue_date);
        if(pub_days<=30) result+=1;
        else if(pub_days<=90) result+=1.5;
        else if(pub_days<=180) result+=1;
        else if(pub_days<=365) result+=0.8;
        else if(pub_days<=365*2) result+=0.6;
        else if(pub_days<=365*3) result+=0.4;
        else result+=0.2;

        if(bsr1==null || bsr1<=0) result+=0;
        else if(bsr1<=100) result+=0.6;
        else if(bsr1<=500) result+=0.8;
        else if(bsr1<=1000) result+=0.9;
        else if(bsr1<=3000) result+=1;
        else if(bsr1<=5000) result+=0.9;
        else result+=0.8;

        if(price==null || price<=0) result+=0;
        else if(price<=10) result+=0.6;
        else if(price<=20) result+=1;
        else if(price<=30) result+=1.5;
        else if(price<=50) result+=1.2;
        else if(price<=100) result+=1;
        else  result+=0.8;

        if(reviews_count==null || reviews_count<=0) result+=0;
        else if(reviews_count<=10) result+=0.2;
        else if(reviews_count<=20) result+=0.4;
        else if(reviews_count<=50) result+=0.6;
        else if(reviews_count<=100) result+=0.8;
        else if(reviews_count<=200) result+=1;
        else result+=0.8;

        if(stars==null || stars<=0) result+=0;
        else if(stars<=2) result+=0.5;
        else if(stars<=3) result+=0.7;
        else if(stars<=3.5) result+=0.9;
        else if(stars<=4) result+=1;
        else if(stars<=4.2) result+=0.8;
        else if(stars<=4.5) result+=0.6;
        else result+=0.4;

        if(offer_listing==null || offer_listing<=0) result+=0;
        else if(offer_listing<=2) result+=0.4;
        else if(offer_listing<=4) result+=0.6;
        else if(offer_listing<=6) result+=0.8;
        else if(offer_listing<=8) result+=1;
        else result+=0.8;

        if(ship==null) result+=0.4;
        else if(ship.equals("FBM")) result+=1;
        else if(ship.equals("FBA")) result+=0.8;
        else if(ship.equals("AMZ")) result+=0.6;
        else result+=0.4;

        if(is_best_seller==null) result+=0;
        else if(is_best_seller==1) result+=0.5;

        return (long) Math.ceil(result);
    }
}