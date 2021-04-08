package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.core.JobDefinition;
import com.github.vitalibo.grapes.processing.core.io.SocialNetworkInputFormat;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

@RequiredArgsConstructor
public class SocialNetworkImportJob implements JobDefinition {

    @Override
    public Job defineJob(Configuration configuration, String[] args) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJobName("Social Network Importing Phase");
        job.setJarByClass(SocialNetworkImportJob.class);
        job.setInputFormatClass(SocialNetworkInputFormat.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(ArrayPrimitiveWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job;
    }

}
