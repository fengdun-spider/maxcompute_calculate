package com.aliyun.odps.mapred.open.example;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Job;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.pipeline.Pipeline;

import java.io.IOException;
import java.util.Iterator;
public class WordCountPipelineTest {
    public static class TokenizerMapper extends MapperBase {
        Record word;
        Record one;
        @Override
        public void setup(TaskContext context) throws IOException {
            word = context.createMapOutputKeyRecord();
            one = context.createMapOutputValueRecord();
            one.setBigint(0, 1L);
        }
        @Override
        public void map(long recordNum, Record record, TaskContext context)
                throws IOException {
            for (int i = 0; i < record.getColumnCount(); i++) {
                String[] words = record.get(i).toString().split("\\s+");
                for (String w : words) {
                    word.setString(0, w);
                    context.write(word, one);
                }
            }
        }
    }
    public static class SumReducer extends ReducerBase {
        private Record value;
        @Override
        public void setup(TaskContext context) throws IOException {
            value = context.createOutputValueRecord();
        }
        @Override
        public void reduce(Record key, Iterator<Record> values, TaskContext context)
                throws IOException {
            long count = 0;
            while (values.hasNext()) {
                Record val = values.next();
                count += (Long) val.get(0);
            }
            value.set(0, count);
            context.write(key, value);
        }
    }
    public static class IdentityReducer extends ReducerBase {
        private Record result;
        @Override
        public void setup(TaskContext context) throws IOException {
            result = context.createOutputRecord();
        }
        @Override
        public void reduce(Record key, Iterator<Record> values, TaskContext context)
                throws IOException {
            while (values.hasNext()) {
                result.set(0, key.get(0));
                result.set(1, values.next().get(0));
                context.write(result);
            }
        }
    }
    public static void main(String[] args) throws OdpsException {
        if (args.length != 2) {
            System.err.println("Usage: WordCountPipeline <in_table> <out_table>");
            System.exit(2);
        }
        Job job = new Job();
        /***
         * 构造Pipeline的过程中，如果不指定Mapper的OutputKeySortColumns、PartitionColumns、
         * OutputGroupingColumns，框架会默认使用其OutputKey作为此三者的默认配置。
         ***/
        Pipeline pipeline = Pipeline.builder()
                .addMapper(TokenizerMapper.class)
                .setOutputKeySchema(
                        new Column[] { new Column("word", OdpsType.STRING) })
                .setOutputValueSchema(
                        new Column[] { new Column("count", OdpsType.BIGINT) })
                .setOutputKeySortColumns(new String[] { "word" })
                .setPartitionColumns(new String[] { "word" })
                .setOutputGroupingColumns(new String[] { "word" })
                .addReducer(SumReducer.class)
                .setOutputKeySchema(
                        new Column[] { new Column("word", OdpsType.STRING) })
                .setOutputValueSchema(
                        new Column[] { new Column("count", OdpsType.BIGINT)})
                .addReducer(IdentityReducer.class).createPipeline();
        //将pipeline的设置到jobconf中，如果需要设置combiner，是通过jobconf来设置。
        job.setPipeline(pipeline);
        //设置输入输出表。
        job.addInput(TableInfo.builder().tableName(args[0]).build());
        job.addOutput(TableInfo.builder().tableName(args[1]).build());
        //作业提交并等待结束。
        job.submit();
        job.waitForCompletion();
        System.exit(job.isSuccessful() == true ? 0 : 1);
    }
}