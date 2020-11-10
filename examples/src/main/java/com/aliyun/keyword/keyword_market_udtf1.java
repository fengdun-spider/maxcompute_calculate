package com.aliyun.keyword;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string->string,bigint,string,bigint"})
public class keyword_market_udtf1 extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        String keyword = (String) args[0];
        String fields = (String) args[1];
        for (String field: fields.split("\\|\\|\\|")) {
            if (!field.contains("#")){
                continue;
            }
            String[] ond_field_array = field.split("###");
            String count_string = ond_field_array[1];
            Long count;
            if ( count_string==null || count_string.isEmpty()){
                count = null;
            }else{
                count = Long.parseLong(count_string);
            }
            String word = ond_field_array[0];
            forward(keyword, count,word,(long) word.split(" ").length);
        }
    }


    @Override
    public void close() throws UDFException {

    }

}