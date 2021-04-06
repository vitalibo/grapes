package com.github.vitalibo.grapes.processing.core.reducer;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

@RequiredArgsConstructor
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable count;

    public IntSumReducer() {
        this(new IntWritable());
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        count.set(sum);
        context.write(key, count);
    }

}
