package com.github.vitalibo.grapes.processing.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;

@FunctionalInterface
public interface JobDefinition extends JobControlDefinition {

    @Override
    default JobControl defineJobControl(Configuration configuration, String[] args) throws IOException {
        final Job job = defineJob(configuration, args);

        final JobControl jobControl = new JobControl("default");
        ControlledJob controlledJob = new ControlledJob(configuration);
        controlledJob.setJob(job);
        jobControl.addJob(controlledJob);
        return jobControl;
    }

    Job defineJob(Configuration configuration, String[] args) throws IOException;

}
