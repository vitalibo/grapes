package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.core.JobDefinition;
import com.github.vitalibo.grapes.processing.core.mapper.DunbarNumberMapper;
import com.github.vitalibo.grapes.processing.core.reducer.IntSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DunbarNumberJob implements JobDefinition {

    @Override
    public Job defineJob(Configuration configuration, String[] args) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(DunbarNumberJob.class);
        job.setJobName("Dunbar's number");
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(DunbarNumberMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job;
    }

}
