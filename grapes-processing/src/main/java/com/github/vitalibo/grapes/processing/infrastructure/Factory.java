package com.github.vitalibo.grapes.processing.infrastructure;

import com.github.vitalibo.grapes.processing.core.JobDefinition;
import com.github.vitalibo.grapes.processing.core.job.WordCountJob;
import com.github.vitalibo.grapes.processing.infrastructure.conf.HoconConfiguration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

public class Factory {

    @Getter(lazy = true)
    private static final Factory instance = new Factory(
        ConfigFactory.load(), ConfigFactory.parseResources("application.hocon"),
        ConfigFactory.parseResources("default-application.hocon"));

    @Getter
    private final Config defaultConfiguration;

    Factory(Config... configs) {
        this.defaultConfiguration = Arrays.stream(configs)
            .reduce(Config::withFallback)
            .orElseThrow(IllegalStateException::new)
            .resolve();
    }

    public Job createWordCountJob(String[] args) throws IOException {
        return createJob(new WordCountJob(), args);
    }

    private Job createJob(JobDefinition definition, String[] args) throws IOException {
        final Configuration configuration = HoconConfiguration.parseHocon(defaultConfiguration, args[0]);
        GenericOptionsParser parser = new GenericOptionsParser(configuration, args);
        return definition.defineJob(configuration, parser.getRemainingArgs());
    }

}
