package com.github.vitalibo.grapes.processing.core.mapper;

import com.github.vitalibo.grapes.processing.core.io.model.MultipleInputWritable;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@RequiredArgsConstructor
public class MultipleInputMapper extends Mapper<IntWritable, ArrayPrimitiveWritable, IntWritable, MultipleInputWritable<ArrayPrimitiveWritable>> {

    private final MultipleInputWritable<ArrayPrimitiveWritable> output;

    public MultipleInputMapper() {
        this(new MultipleInputWritable.ArrayPrimitiveWritable((byte) 2));
    }

    @Override
    protected void map(IntWritable key, ArrayPrimitiveWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, output.wrap(value));
    }

}
