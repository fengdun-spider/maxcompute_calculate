package com.aliyun.odps.udf;

import com.aliyun.odps.udf.annotation.Resolve;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string->string,string,bigint,double"})
public class amazon_market_udtf1 extends UDTF {

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
            String cate = (String) ond_field_array[0];
            String products_count_string = (String) ond_field_array[2];
            Long products_count;
            if ( products_count_string==null || products_count_string.isEmpty()){
                products_count = null;
            }else{
                products_count = Long.parseLong(products_count_string);
            }
            Double sales_proportion;
            String sales_proportion_string = (String) ond_field_array[1];
            if (sales_proportion_string==null || sales_proportion_string.isEmpty()){
                sales_proportion = null;
            }else{
                sales_proportion = Double.parseDouble(sales_proportion_string);
            }
            forward(node_id, cate,products_count,sales_proportion);
        }
    }

    @Override
    public void close() throws UDFException {

    }

}