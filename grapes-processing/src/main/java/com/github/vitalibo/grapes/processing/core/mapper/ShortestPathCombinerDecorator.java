package com.github.vitalibo.grapes.processing.core.mapper;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

@RequiredArgsConstructor
public class ShortestPathCombinerDecorator extends Mapper<IntWritable, ArrayPrimitiveWritable, IntWritable, ArrayPrimitiveWritable> {

    private final Function<Context, MultipleOutputs<IntWritable, ? extends Writable>> multipleOutputFactory;

    private MultipleOutputs<IntWritable, ? extends Writable> multipleOutput;
    private Counter counter;
    private Integer target;

    public ShortestPathCombinerDecorator() {
        this(MultipleOutputs::new);
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        multipleOutput = multipleOutputFactory.apply(context);
        counter = context.getCounter("Dijkstra Algorithm", "Unique paths");
        target = Optional.ofNullable(configuration.get("grapes.vertex.target"))
            .map(Integer::parseInt).orElse(null);
    }

    @Override
    public void map(IntWritable key, ArrayPrimitiveWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
        multipleOutput.write("cache", key, NullWritable.get());

        if (target != null && target == key.get()) {
            counter.increment(1);
            multipleOutput.write("sixdegrees", key, value);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutput.close();
    }

}
