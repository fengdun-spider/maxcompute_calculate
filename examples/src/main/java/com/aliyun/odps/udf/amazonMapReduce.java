package com.aliyun.odps.udf;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aliyun.odps.utils.tools.*;

public class amazonMapReduce {
    public static class TokenizerMapper extends MapperBase {
        private Record key;
        private Record value;
        @Override
        public void setup(TaskContext context) throws IOException {
            key = context.createMapOutputKeyRecord();
            value = context.createMapOutputValueRecord();
            System.out.println("TaskID:" + context.getTaskID().toString());
        }
        @Override
        public void map(long recordNum, Record record, TaskContext context)
                throws IOException {
            key.set(0,record.get("product_node"));
            value.set(0,record.get("product_node"));
            value.set(1,record.get("brand"));
            value.set(2,record.get("month_sold_cnt"));
            value.set(3,record.get("ship"));
            value.set(4,record.get("issue_date"));
            value.set(5,record.get("price"));
            value.set(6,record.get("stars"));
            value.set(7,record.get("reviews_count"));
            context.write(key, value);
        }
    }

    public static class SumReducer extends ReducerBase {
        private Record result = null;
        @Override
        public void setup(TaskContext context) throws IOException {
            result = context.createOutputRecord();
        }
        @Override
        public void reduce(Record key, Iterator<Record> values, TaskContext context)
                throws IOException {
            //所有asin数量
            long ana_total_asin_cnt = 0;
            // 类目下总销量
            long ana_month_sold_cnt_sum = 0;
            // 品牌对asin数量
            Map<String,Long> brand_asin_cnt_map = new HashMap<>();
            // 品牌对月数量
            Map<String,Long> brand_month_sold_cnt_map = new HashMap<>();
            // fbm 个数
            long ana_ship_fbm_cnt_sum = 0;
            // fba 个数
            long ana_ship_fba_cnt_sum = 0;
            // amz 个数
            long ana_ship_amz_cnt_sum = 0;
            // 日期对应的asin个数
            Map<String,Long> issue_date_asin_cnt_map = new HashMap<>();
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

            while (values.hasNext()) {
                ana_total_asin_cnt +=1;
                Record value = values.next();
                String brand = (String) value.get("brand");
                Object month_sold_cnt = value.get("month_sold_cnt");
                long month_sold_cnt_long = cast_null_negative_long(month_sold_cnt);
                String ship = (String) value.get("ship");
                String issue_date = (String) value.get("issue_date");
                Object price = value.get("price");
                double price_double = cast_null_negative_double(price);
                String price_range = get_price_range(price_double);
                Object stars = value.get("stars");
                double stars_double = cast_null_negative_double(stars);
                String stars_range = get_stars_range(stars_double);
                Object reviews_count = value.get("reviews_count");
                long reviews_count_long = cast_null_negative_long(reviews_count);
                String reviews_count_range = get_reviews_count_range(reviews_count_long);
                // asin有月销量的时候，聚合计算总月销量，不同区间范围的月销量
                if (month_sold_cnt_long>0) {
                    ana_month_sold_cnt_sum = ana_month_sold_cnt_sum + month_sold_cnt_long;
                    map_key_add_value(price_range_month_sold_cnt_map,price_range,month_sold_cnt_long);
                    map_key_add_value(stars_range_month_sold_cnt_map,stars_range,month_sold_cnt_long);
                    map_key_add_value(reviews_cnt_range_month_sold_cnt_map,reviews_count_range,month_sold_cnt_long);
                }
                // asin有品牌的时候，聚合计算品牌asin数，品牌月销量
                if(brand!=null && !brand.isEmpty()){
                    brand = handle_brand_string(brand);
                    map_key_add_value(brand_asin_cnt_map,brand,1);
                    if (month_sold_cnt_long>0) {
                        map_key_add_value(brand_month_sold_cnt_map, brand, month_sold_cnt_long);
                    }
                }
                // asin有物流信息时，聚合计算amz，fbm，fba的个数
                if(ship!=null && !ship.isEmpty()){
                    switch (ship){
                        case "FBA":
                            ana_ship_fba_cnt_sum +=1;
                            break;
                        case "FBM":
                            ana_ship_fbm_cnt_sum +=1;
                            break;
                        case "AMZ":
                            ana_ship_amz_cnt_sum +=1;
                            break;
                    }
                }
                // asin有发布日期，聚合发布日期，统计每个月发布asin个数，3前的时间不聚合
                try {
                    if(issue_date!=null && !issue_date.isEmpty() && date_is_new(issue_date,1095)){
                        map_key_add_value(issue_date_asin_cnt_map,issue_date,1);
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                // 各范围区间内的asin个数
                map_key_add_value(price_range_asin_cnt_map,price_range,1);
                map_key_add_value(stars_range_asin_cnt_map,stars_range,1);
                map_key_add_value(reviews_cnt_range_asin_cnt_map,reviews_count_range,1);
            }
            JSONObject result_json = new JSONObject();
            // 类型下所有asin个数
            result_json.put("ana_total_asin_cnt",ana_total_asin_cnt);
            // 类目下总销量
            result_json.put("ana_month_sold_cnt_sum",ana_month_sold_cnt_sum);
            // 前30品牌对应其asin个数
            result_json.put("ana_top30_brand_asin_cnt", sortMapByValues_string(brand_asin_cnt_map, 30));
            // 前30品牌对应其月销量占比
            result_json.put("ana_top30_brand_sold_cnt_rate",sortMapByValues_string(brand_asin_cnt_map,30,ana_month_sold_cnt_sum));
            // fma个数
            result_json.put("ana_ship_fba_cnt_sum",ana_ship_fba_cnt_sum);
            // fbm个数
            result_json.put("ana_ship_fbm_cnt_sum",ana_ship_fbm_cnt_sum);
            // amz个数
            result_json.put("ana_ship_amz_cnt_sum",ana_ship_amz_cnt_sum);
            // 上架趋势
            result_json.put("ana_date_exhibit_asin_cnt_sum",map_2_string(issue_date_asin_cnt_map));
            // 价格分布
            result_json.put("ana_price_range_asin_cnt",map_2_string(price_range_asin_cnt_map));
            result_json.put("ana_price_range_month_sold_cnt",map_2_string(price_range_month_sold_cnt_map));
            // 星级分布
            result_json.put("ana_stars_range_asin_cnt",map_2_string(stars_range_asin_cnt_map));
            result_json.put("ana_stars_range_month_sold_cnt",map_2_string(stars_range_month_sold_cnt_map));
            // 评分数分布
            result_json.put("ana_reviews_cnt_range_asin_cnt",map_2_string(reviews_cnt_range_asin_cnt_map));
            result_json.put("ana_reviews_cnt_range_month_sold_cnt",map_2_string(reviews_cnt_range_month_sold_cnt_map));
            result.set(0,key.get("product_node"));
            result.set(1,result_json.toString());
            System.out.println(result_json);
            context.write(result);
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("please spec pt like 2020-09-27");
            System.exit(2);
        }
        String pt = args[0];
        String pattern = "pt=\\d\\d\\d\\d-\\d\\d-\\d\\d";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(pt);
        if(!m.find()){
            System.err.println("please spec pt like 2020-09-27");
            System.exit(2);
        }
        JobConf job = new JobConf();
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);
        //设置mapper中间结果的key和value的schema, mapper的中间结果输出也是record的形式。
        job.setMapOutputKeySchema(SchemaUtils.fromString("product_node:String"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("product_node:String,brand:String," +
                "month_sold_cnt:bigint,ship:String,issue_date:String,price:double,stars:double,reviews_count:bigint"));
        job.setPartitionColumns(new String[] { "product_node" });
        job.setOutputKeySortColumns(new String[] { "product_node" });
        job.setOutputGroupingColumns(new String[] { "product_node" });
        //设置输入和输出的表信息。
        InputUtils.addTable(TableInfo.builder().tableName("listingitem_result").partSpec(pt).build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("wc_out").partSpec(pt).build(), job);
        JobClient.runJob(job);
    }
}