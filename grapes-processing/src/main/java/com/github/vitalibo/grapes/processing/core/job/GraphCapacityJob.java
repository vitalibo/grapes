package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.core.JobDefinition;
import com.github.vitalibo.grapes.processing.core.io.model.GraphElementWritable;
import com.github.vitalibo.grapes.processing.core.mapper.GraphElementMapper;
import com.github.vitalibo.grapes.processing.core.reducer.GraphCapacityReducer;
import com.github.vitalibo.grapes.processing.core.util.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class GraphCapacityJob implements JobDefinition {

    @Override
    public Job defineJob(Configuration configuration, String[] args) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(GraphCapacityJob.class);
        job.setJobName("Graph Capacity");
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(GraphElementMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(GraphElementWritable.class);
        job.setNumReduceTasks(20);
        job.setReducerClass(GraphCapacityReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, Paths.createTempDirectory(configuration));

        return job;
    }

}
