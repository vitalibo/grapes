package com.github.vitalibo.grapes.processing.core.reducer;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Optional;

@RequiredArgsConstructor
public class ShortestPathCombiner extends Reducer<IntWritable, ArrayPrimitiveWritable, IntWritable, ArrayPrimitiveWritable> {

    private final ArrayPrimitiveWritable output;
    private Integer target;

    public ShortestPathCombiner() {
        this(new ArrayPrimitiveWritable());
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        target = Optional.ofNullable(configuration.get("grapes.vertex.target"))
            .map(Integer::parseInt).orElse(null);
    }

    @Override
    public void reduce(IntWritable key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
        if (target != null && target == key.get()) {
            super.reduce(key, values, context);
            return;
        }

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
