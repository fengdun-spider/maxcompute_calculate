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
@Resolve({"STRING,ARRAY<STRUCT<KEYWORD:STRING,PAGE:BIGINT,LIST_ID:BIGINT,RANK_ID:BIGINT,ASIN:STRING," +
        "PRICE:DOUBLE,STARS:DOUBLE,REVIEWS_COUNT:BIGINT,PRODUCT_TITLE:STRING,IS_AD:BIGINT,SHIP:STRING," +
        "ALL_COUNT:BIGINT,ADD_DATE_TIME:DATETIME,NODE_ID:STRING,MONTH_SOLD_CNT:BIGINT>>->string,string," +
        "bigint,bigint,bigint,bigint,bigint,string,datetime"})
public class KeyWordUdtf extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException, IOException {
        String keyword = (String) args[0];
        List<Struct> all_item = (List) args[1];
        for ( Struct item : all_item) {
            String asin = (String) item.getFieldValue("asin");
            Long page_id = (Long) item.getFieldValue("page");
            Long rank_id = (Long) item.getFieldValue("rank_id");
            Long all_count = (Long) item.getFieldValue("all_count");
            Long list_id = (Long) item.getFieldValue("list_id");
            Long is_ad = (Long) item.getFieldValue("is_ad");
            String node_id = (String) item.getFieldValue("node_id");
            if (node_id.isEmpty()) node_id =null;
            Date add_date_time = (Date) item.getFieldValue("add_date_time");
            forward(asin,keyword,page_id,list_id,rank_id,all_count,is_ad,node_id,add_date_time);
        }
    }

    @Override
    public void close() throws UDFException {

    }

}