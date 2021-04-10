package com.github.vitalibo.grapes.processing.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;

@FunctionalInterface
public interface JobControlDefinition {

    JobControl defineJobControl(Configuration configuration, String[] args) throws IOException;

}
