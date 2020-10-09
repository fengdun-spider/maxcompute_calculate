package com.aliyun.odps.examples.udf;

import com.aliyun.odps.Yieldable;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDJ;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.Iterator;

// TODO define output types, e.g. "->string,bigint,string".
@Resolve({"->"})
public class handle_duplicate extends UDJ {

    /**
     * Will be called prior to the data processing phase. User could implement
     * this method to do initialization work.
     */
    @Override
    public void setup(ExecutionContext executionContext, DataAttributes dataAttributes) {

    }

    /**
     * Override this method to implement join logic.
     *
     * @param key    Current join key
     * @param left   Group of records of left table corresponding to the current key
     * @param right  Group of records of right table corresponding to the current key
     * @param output Used to output the result of UDJ
     */
    @Override
    public void join(Record key, Iterator<Record> left, Iterator<Record> right, Yieldable<Record> output) {

        // TODO
    }

    @Override
    public void close() {

    }

}