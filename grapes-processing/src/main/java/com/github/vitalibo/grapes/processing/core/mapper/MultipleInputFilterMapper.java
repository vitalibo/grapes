package com.github.vitalibo.grapes.processing.core.mapper;

import com.github.vitalibo.grapes.processing.core.io.model.MultipleInputWritable;
import com.github.vitalibo.grapes.processing.core.util.Paths;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class MultipleInputFilterMapper extends Mapper<IntWritable, ArrayPrimitiveWritable, IntWritable, MultipleInputWritable<ArrayPrimitiveWritable>> {

    private final MultipleInputWritable<ArrayPrimitiveWritable> output;
    private final Function<URI[], Stream<String>> cacheFilesReader;

    private Set<Integer> items;

    public MultipleInputFilterMapper() {
        this(new MultipleInputWritable.ArrayPrimitiveWritable((byte) 1), Paths::cacheFilesAsTextLines);
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        items = cacheFilesReader.apply(context.getCacheFiles())
            .map(Integer::parseInt)
            .collect(Collectors.toSet());
    }

    @Override
    public void map(IntWritable key, ArrayPrimitiveWritable inValue, Context context) throws IOException, InterruptedException {
        if (items.contains(key.get())) {
            context.write(key, output.wrap(inValue));
        }
    }

}
