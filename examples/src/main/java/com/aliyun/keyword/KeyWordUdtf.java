package com.aliyun.keyword;

import com.aliyun.odps.data.Struct;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.io.IOException;
import java.util.Date;
import java.util.List;


// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"STRING,BIGINT,ARRAY<STRUCT<keyword:STRING,page:BIGINT,list_id:BIGINT,asin:STRING,pic_url:STRING," +
        "price:DOUBLE,stars:DOUBLE,reviews_count:BIGINT,product_title:STRING,is_ad:BIGINT,ship:STRING," +
        "ship_fee:DOUBLE,add_date_time:DATETIME>>->string,string,bigint,bigint,bigint,bigint,bigint,datetime"})
public class KeyWordUdtf extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException, IOException {
        String keyword = (String) args[0];
        Long all_count = (Long) args[1];
        List<Struct> all_item = (List) args[2];
        long rank_id = 0L;
        for ( Struct item : all_item) {
            String asin = (String) item.getFieldValue("asin");
            Long page_id = (Long) item.getFieldValue("page");
            rank_id +=1;
            Long list_id = (Long) item.getFieldValue("list_id");
            Long is_ad = (Long) item.getFieldValue("is_ad");
            Date add_date_time = (Date) item.getFieldValue("add_date_time");
            forward(asin,keyword,page_id,list_id,rank_id,all_count,is_ad,add_date_time);
        }
    }

    @Override
    public void close() throws UDFException {

    }

}