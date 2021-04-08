package com.github.vitalibo.grapes.processing;

import com.github.vitalibo.grapes.processing.infrastructure.Factory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.mapreduce.Job;

@Slf4j
@RequiredArgsConstructor
public class Driver {

    private final Factory factory;

    public Driver() {
        this(Factory.getInstance());
    }

    public void run(String[] args) throws Exception {
        try {
            final Job job; // NOPMD
            switch (args[0]) {
                case "wordcount":
                    job = factory.createWordCountJob(args);
                    break;
                case "socnetimp":
                    job = factory.createSocialNetworkImportJob(args);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown job name");
            }

            job.submit();
            job.waitForCompletion(true);
        } catch (Exception e) {
            logger.error("MapReduce Job failed execution", e);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        new Driver().run(args);
    }

}
