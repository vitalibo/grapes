package com.github.vitalibo.grapes.processing.infrastructure.vk;

import com.github.vitalibo.grapes.processing.core.io.UserInputSplit;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

@RequiredArgsConstructor
public class VkUserRecordReader extends RecordReader<IntWritable, ArrayPrimitiveWritable> {

    private final IntWritable outKey;
    private final ArrayPrimitiveWritable outValue;
    private final Queue<Integer> backlog;
    private final Queue<Map.Entry<Integer, List<Integer>>> queue;
    private final AtomicReference<Map.Entry<Integer, List<Integer>>> current;
    private final BiFunction<UserInputSplit, Configuration, VkClient> vkClientFactory;

    private UserInputSplit split;
    private VkClient vk;

    public VkUserRecordReader() {
        this(new IntWritable(), new ArrayPrimitiveWritable(), new LinkedList<>(),
            new LinkedList<>(), new AtomicReference<>(), VkClient::newInstance);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        initialize((UserInputSplit) split, context);
    }

    private void initialize(UserInputSplit split, TaskAttemptContext context) {
        final Configuration configuration = context.getConfiguration();
        this.vk = vkClientFactory.apply(split, configuration);
        this.split = split;
        Arrays.stream(split.getUsers()).forEach(backlog::add);
    }

    @Override
    public boolean nextKeyValue() {
        if (queue.isEmpty() && backlog.isEmpty()) {
            return false;
        }

        if (queue.isEmpty()) {
            Map<Integer, List<Integer>> friends = vk.friends(backlog::poll);
            queue.addAll(friends.entrySet());
        }

        current.set(queue.poll());
        return current.get() != null;
    }

    @Override
    public IntWritable getCurrentKey() {
        final int key = current.get()
            .getKey();

        outKey.set(key);
        return outKey;
    }

    @Override
    public ArrayPrimitiveWritable getCurrentValue() {
        final int[] value = current.get()
            .getValue()
            .stream()
            .mapToInt(o -> o)
            .toArray();

        outValue.set(value);
        return outValue;
    }

    @Override
    public float getProgress() {
        return (float) (split.getLength() - backlog.size() - queue.size()) / split.getLength();
    }

    @Override
    public void close() {
    }

}
