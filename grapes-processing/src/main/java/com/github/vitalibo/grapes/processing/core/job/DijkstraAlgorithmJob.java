package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.core.JobControlDefinition;
import com.github.vitalibo.grapes.processing.core.io.model.MultipleInputWritable;
import com.github.vitalibo.grapes.processing.core.io.model.VisitedVertexWritable;
import com.github.vitalibo.grapes.processing.core.mapper.AdjacencyVertexMapper;
import com.github.vitalibo.grapes.processing.core.mapper.AfterReduceMapper;
import com.github.vitalibo.grapes.processing.core.mapper.MultipleInputFilterMapper;
import com.github.vitalibo.grapes.processing.core.mapper.MultipleInputMapper;
import com.github.vitalibo.grapes.processing.core.reducer.JoinReducer;
import com.github.vitalibo.grapes.processing.core.reducer.ShortestPathCombiner;
import com.github.vitalibo.grapes.processing.core.util.CacheFileExcludePathFilter;
import com.github.vitalibo.grapes.processing.core.util.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Objects;

@SuppressWarnings("PMD.CloseResource")
public class DijkstraAlgorithmJob implements JobControlDefinition {

    @Override
    public JobControl defineJobControl(Configuration configuration, String[] args) throws IOException {
        final JobControl jobControl = new JobControl("Dijkstra Group");
        final int phase = Integer.parseInt(args[1]);

        final ControlledJob job1 = defineSearchPreparationJob(configuration, phase, args[2], args[3] + "/phase");
        jobControl.addJob(job1);

        final ControlledJob job2 = defineSearchJob(configuration, phase, args[3] + "/phase");
        job2.addDependingJob(job1);
        jobControl.addJob(job2);

        return jobControl;
    }

    static ControlledJob defineSearchPreparationJob(Configuration configuration, int phase, String inDir, String dir) throws IOException {
        final FileSystem fs = FileSystem.get(configuration);

        if (phase == 1) {
            String vertex = Objects.requireNonNull(
                configuration.get("grapes.vertex.initial"), "Initial vertex should be defined");
            Paths.createTextFile(fs, dir + "0/out/cache-r-00000", vertex);
        }

        final Job job = Job.getInstance(configuration);
        job.setJobName(String.format("Dijkstra's Algorithm [ phase %s (1/2) ] ", phase));
        job.setJarByClass(DijkstraAlgorithmJob.class);
        MultipleInputs.addInputPath(job, new Path(inDir), SequenceFileInputFormat.class,
            MultipleInputFilterMapper.class);
        MultipleInputs.addInputPath(job, new Path(dir + (phase - 1) + "/out"),
            SequenceFileInputFormat.class, MultipleInputMapper.class);
        SequenceFileInputFormat.setInputPathFilter(job, CacheFileExcludePathFilter.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MultipleInputWritable.ArrayPrimitiveWritable.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(VisitedVertexWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(dir + phase + "/tmp"));

        for (Path path : Paths.listFiles(fs, dir + (phase - 1) + "/out", true)) {
            if (path.getName().startsWith("cache-")) {
                job.addCacheFile(path.toUri());
            }
        }

        final ControlledJob controlledJob = new ControlledJob(configuration);
        controlledJob.setJob(job);
        return controlledJob;
    }

    static ControlledJob defineSearchJob(Configuration configuration, int phase, String dir) throws IOException {
        final Job job = Job.getInstance(configuration);
        job.setJobName(String.format("Dijkstra's Algorithm [ phase %s (2/2) ] ", phase));
        job.setJarByClass(DijkstraAlgorithmJob.class);
        SequenceFileInputFormat.addInputPath(job, new Path(dir + phase + "/tmp"));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(AdjacencyVertexMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        job.setCombinerClass(ShortestPathCombiner.class);

        ChainReducer.setReducer(job, ShortestPathCombiner.class, IntWritable.class, ArrayPrimitiveWritable.class,
            IntWritable.class, ArrayPrimitiveWritable.class, configuration);
        ChainReducer.addMapper(job, AfterReduceMapper.class, IntWritable.class, ArrayPrimitiveWritable.class,
            IntWritable.class, ArrayPrimitiveWritable.class, configuration);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(ArrayPrimitiveWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(dir + phase + "/out"));
        MultipleOutputs.addNamedOutput(job, "cache", TextOutputFormat.class,
            IntWritable.class, NullWritable.class);

        final ControlledJob controlledJob = new ControlledJob(configuration);
        controlledJob.setJob(job);
        return controlledJob;
    }

}
