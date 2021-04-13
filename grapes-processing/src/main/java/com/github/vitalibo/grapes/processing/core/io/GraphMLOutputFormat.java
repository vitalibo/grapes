package com.github.vitalibo.grapes.processing.core.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GraphMLOutputFormat extends FileOutputFormat<IntWritable, ArrayPrimitiveWritable> {

    @Override
    public RecordWriter<IntWritable, ArrayPrimitiveWritable> getRecordWriter(TaskAttemptContext job) throws IOException {
        final Configuration configuration = job.getConfiguration();
        Path file = this.getDefaultWorkFile(job, ".xml");
        FileSystem fs = file.getFileSystem(configuration); // NOPMD
        FSDataOutputStream out = fs.create(file, false);
        return new GraphMLRecordWriter(out);
    }

}
