package com.github.vitalibo.grapes.processing.core.mapper;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@RequiredArgsConstructor
public class DunbarNumberMapper extends Mapper<IntWritable, ArrayPrimitiveWritable, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    private final Text output;

    public DunbarNumberMapper() {
        this(new Text());
    }

    @Override
    protected void map(IntWritable key, ArrayPrimitiveWritable value, Context context) throws IOException, InterruptedException {
        int[] neighbours = (int[]) value.get();
        output.set(String.valueOf(neighbours.length));
        context.write(output, ONE);
    }

}
