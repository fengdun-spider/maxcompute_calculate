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
import java.util.*;

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
            String words = "";
            String product_title = (String) record.get("product_title");
            if (product_title!=null && !product_title.isEmpty()) words = words+" " + product_title;
            String bullet_point1 = (String) record.get("bullet_point1");
            if (bullet_point1!=null && !bullet_point1.isEmpty()) words = words+" " + bullet_point1;
            String bullet_point2 = (String) record.get("bullet_point2");
            if (bullet_point2!=null && !bullet_point2.isEmpty()) words = words+" " + bullet_point2;
            String bullet_point3 = (String) record.get("bullet_point3");
            if (bullet_point3!=null && !bullet_point3.isEmpty()) words = words+" " + bullet_point3;
            String bullet_point4 = (String) record.get("bullet_point4");
            if (bullet_point4!=null && !bullet_point4.isEmpty()) words = words+" " + bullet_point4;
            String bullet_point5 = (String) record.get("bullet_point5");
            if (bullet_point5!=null && !bullet_point5.isEmpty()) words = words+" " + bullet_point5;
            value.set(0,words);
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
            // price，sold_cnt_list List
            ArrayList<Double>  price_list = new ArrayList<>();
            ArrayList<Long>  month_sold_cnt_list = new ArrayList<>();
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
            // word-cnt map
            Map<String,Long> word_cnt_map = new HashMap<>();
            while (values.hasNext()) {
                Record value = values.next();
                // 类目下所有asin个数
                ana_total_asin_cnt +=1;
                // 标题,五点描述词频统计
                split_string(word_cnt_map,(String) value.get(0));
                String brand = (String) value.get("brand");
                if(brand==null || brand.isEmpty()){
                    brand = "未知品牌";
                }
                brand = brand.replace("#","*").replace("|","!");

                // asin的月销量，可能为null
                Long month_sold_cnt_long = (Long) value.get("month_sold_cnt");
                String ship = (String) value.get("ship");
                String issue_date = (String) value.get("issue_date");
                Double price_double = (Double) value.get("price");
                Double stars_double = (Double) value.get("stars");
                // 星级范围
                String stars_range = get_stars_range(stars_double);
                Long reviews_count_long = (Long) value.get("reviews_count");
                // 评论数范围
                String reviews_count_range = get_reviews_count_range(reviews_count_long);


                // asin有月销量的时候，聚合计算总月销量，不同区间范围的月销量
                if (month_sold_cnt_long!=null && month_sold_cnt_long>0) {
                    ana_month_sold_cnt_sum = ana_month_sold_cnt_sum + month_sold_cnt_long;
                    map_key_add_value(brand_month_sold_cnt_map, brand, month_sold_cnt_long);
                    map_key_add_value(stars_range_month_sold_cnt_map,stars_range,month_sold_cnt_long);
                    map_key_add_value(reviews_cnt_range_month_sold_cnt_map,reviews_count_range,month_sold_cnt_long);
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
                if(date_is_new(issue_date,1095)){
                    map_key_add_value(issue_date_asin_cnt_map,issue_date.replaceAll("-\\d+$","-01"),1);
                }
                // 存储价格和月销量
                if (price_double !=null && price_double>0){
                    price_list.add(price_double);
                    month_sold_cnt_list.add(month_sold_cnt_long);
                }
                // 各范围区间内的asin个数
                map_key_add_value(brand_asin_cnt_map,brand,1);
                map_key_add_value(stars_range_asin_cnt_map,stars_range,1);
                map_key_add_value(reviews_cnt_range_asin_cnt_map,reviews_count_range,1);
            }
            JSONObject result_json = new JSONObject();
            // 类目下所有asin个数
            result_json.put("ana_total_asin_cnt",ana_total_asin_cnt);
            // 类目下总销量
            result_json.put("ana_month_sold_cnt_sum",ana_month_sold_cnt_sum);
            // 前30品牌分布
            result_json.put("ana_top30_brand",sortMapByValues_string_combine(brand_month_sold_cnt_map,30,ana_month_sold_cnt_sum,brand_asin_cnt_map));
            // fma个数
            result_json.put("ana_ship_fba_cnt_sum",ana_ship_fba_cnt_sum);
            // fbm个数
            result_json.put("ana_ship_fbm_cnt_sum",ana_ship_fbm_cnt_sum);
            // amz个数
            result_json.put("ana_ship_amz_cnt_sum",ana_ship_amz_cnt_sum);
            // 上架趋势
            result_json.put("ana_date_exhibit_asin_cnt_sum",map_2_string(issue_date_asin_cnt_map));
            // 价格分布
            if (price_list.size()>0){
                double price_avg = list_double_avg(price_list);
                for (int i=0;i<price_list.size();i++){
                    Double price = price_list.get(i);
                    String price_range = get_price_range(price,price_avg);
                    Long month_sold = month_sold_cnt_list.get(i);
                    map_key_add_value(price_range_asin_cnt_map,price_range,1);
                    if(month_sold!=null && month_sold>0){
                        map_key_add_value(price_range_month_sold_cnt_map,price_range,month_sold);
                    }
                }
            result_json.put("ana_price_range",map_2_string_combine(price_range_month_sold_cnt_map,ana_month_sold_cnt_sum,price_range_asin_cnt_map));
            }else{
                result_json.put("ana_price_range","");
            }
            // 星级分布
            result_json.put("ana_stars_range",map_2_string_combine(stars_range_month_sold_cnt_map,ana_month_sold_cnt_sum,stars_range_asin_cnt_map));
            // 评分数分布
            result_json.put("ana_reviews_cnt_range",map_2_string_combine(reviews_cnt_range_month_sold_cnt_map,ana_month_sold_cnt_sum,reviews_cnt_range_asin_cnt_map));
            // 词频统计
            result_json.put("word_cnt_100",sortMapByValues_string(word_cnt_map,100));
            result.set(0,key.get("product_node"));
            result.set(1,result_json.toJSONString());
//            System.out.println(result_json.toJSONString());
            context.write(result);
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1 && valid_monday_pt(args[0])) {
            System.err.println("please spec pt like pt=2020-09-28,ensure is a past monday date !!!");
            System.exit(2);
        }
        String pt = args[0];
        JobConf job = new JobConf();
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);
        //设置mapper中间结果的key和value的schema, mapper的中间结果输出也是record的形式。
        job.setMapOutputKeySchema(SchemaUtils.fromString("product_node:String"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("words:String,brand:String," +
                "month_sold_cnt:bigint,ship:String,issue_date:String,price:double,stars:double," +
                "reviews_count:bigint"));
        job.setPartitionColumns(new String[] { "product_node" });
        job.setOutputKeySortColumns(new String[] { "product_node" });
        job.setOutputGroupingColumns(new String[] { "product_node" });
        //设置输入和输出的表信息。
        InputUtils.addTable(TableInfo.builder().tableName("listingitem_result").partSpec(pt).build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("listingitem_mapreduce").partSpec(pt).build(), job);
        JobClient.runJob(job);
    }
}