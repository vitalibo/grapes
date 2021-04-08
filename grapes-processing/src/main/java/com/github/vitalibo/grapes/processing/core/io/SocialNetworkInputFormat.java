package com.github.vitalibo.grapes.processing.core.io;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class SocialNetworkInputFormat extends InputFormat<IntWritable, ArrayPrimitiveWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) {
        final Configuration configuration = context.getConfiguration();
        int startId = configuration.getInt("grapes.split.startId", 1);
        int endId = configuration.getInt("grapes.split.endId", 1000);
        int chunks = configuration.getInt("grapes.split.numbs" /* MRJobConfig.NUM_MAPS */, 1);
        int chunkSize = (endId - startId) / chunks;
        logger.info("Total generated split chunks {}. Average chunk size {} IDs.", chunks, chunkSize);

        return IntStream.range(0, chunks)
            .mapToObj(i -> new UserInputSplit(i,
                IntStream.range(startId + i * chunkSize, i + 1 == chunks ? endId : startId + (i + 1) * chunkSize)
                    .toArray()))
            .collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings("unchecked")
    @SneakyThrows({InstantiationException.class, IllegalAccessException.class})
    public RecordReader<IntWritable, ArrayPrimitiveWritable> createRecordReader(InputSplit split,
                                                                                TaskAttemptContext context) {
        final Configuration configuration = context.getConfiguration();
        Class<? extends RecordReader> cls = configuration.getClass(
            "grapes.recordReader.class", null, RecordReader.class);
        Objects.requireNonNull(cls, "Record reader class must be set.");
        return cls.newInstance();
    }

}
