package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.core.JobDefinition;
import com.github.vitalibo.grapes.processing.core.mapper.TokenizerMapper;
import com.github.vitalibo.grapes.processing.core.reducer.IntSumReducer;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

@RequiredArgsConstructor
public class WordCountJob implements JobDefinition {

    @Override
    public Job defineJob(Configuration configuration, String[] args) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJobName("Word Count");
        job.setJarByClass(WordCountJob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (args.length > 3) {
            job.addCacheFile(new Path(args[3]).toUri());
            job.getConfiguration().setBoolean("grapes.skip.patterns", true);
        }

        return job;
    }

}
