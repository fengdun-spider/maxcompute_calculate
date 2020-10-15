package com.aliyun.odps.udf;

import com.aliyun.odps.udf.annotation.Resolve;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string->string,bigint,string"})
public class amazon_market_udtf2 extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        String node_id = (String) args[0];
        String fields = (String) args[1];
        for (String field: fields.split("\\|\\|\\|")) {
            if (!field.contains("#")){
                continue;
            }
            Object[] ond_field_array = field.split("###");
            String products_count_string = (String) ond_field_array[1];
            Long products_count;
            if ( products_count_string==null || products_count_string.isEmpty()){
                products_count = null;
            }else{
                products_count = Long.parseLong(products_count_string);
            }
            String data = (String) ond_field_array[0];
            forward(node_id, products_count,data);
        }
    }

    @Override
    public void close() throws UDFException {

    }

}