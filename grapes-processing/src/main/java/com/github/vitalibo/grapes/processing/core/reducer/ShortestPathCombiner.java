package com.github.vitalibo.grapes.processing.core.reducer;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

@RequiredArgsConstructor
public class ShortestPathCombiner extends Reducer<IntWritable, ArrayPrimitiveWritable, IntWritable, ArrayPrimitiveWritable> {

    private final ArrayPrimitiveWritable output;

    public ShortestPathCombiner() {
        this(new ArrayPrimitiveWritable());
    }

    @Override
    protected void reduce(IntWritable key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
        int distance = Integer.MAX_VALUE;
        int[] minPath = new int[0];

        for (ArrayPrimitiveWritable value : values) {
            int[] path = (int[]) value.get();
            if (path.length < distance) {
                distance = path.length;
                minPath = path;
            }
        }

        output.set(minPath);
        context.write(key, output);
    }

}
