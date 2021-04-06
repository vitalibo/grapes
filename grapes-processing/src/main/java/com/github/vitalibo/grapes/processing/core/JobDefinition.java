package com.github.vitalibo.grapes.processing.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

@FunctionalInterface
public interface JobDefinition {

    Job defineJob(Configuration configuration, String[] args) throws IOException;

}
