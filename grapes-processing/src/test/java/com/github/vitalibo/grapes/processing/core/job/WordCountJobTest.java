package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.MapReduceSuiteBase;
import com.github.vitalibo.grapes.processing.TestHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Objects;

public class WordCountJobTest extends MapReduceSuiteBase {

    private WordCountJob definition;

    @BeforeClass
    public void setUp() {
        definition = new WordCountJob();
    }

    @Test
    public void testJob() throws Exception {
        fs.sync(TestHelper.resourcePath("input/"), "/input");

        Job job = definition.defineJob(cluster.getConfiguration(), new String[]{"wordcount", "/input", "/output"});
        job.setNumReduceTasks(2);
        job.submit();
        job.waitForCompletion(true);

        Assert.assertTrue(job.isSuccessful());
        File path = TestHelper.resourceAsFile(TestHelper.resourcePath("output/"));
        for (String name : Objects.requireNonNull(path.list())) {
            Assert.assertEquals(fs.open("/output/" + name),
                TestHelper.resourceAsString(TestHelper.resourcePath("output/" + name)));
        }
    }

    @Test
    public void testJobCaseInsensitive() throws Exception {
        fs.sync(TestHelper.resourcePath("input/"), "/input");
        Configuration configuration = cluster.getConfiguration();
        configuration.setBoolean("grapes.case.sensitive", false);

        Job job = definition.defineJob(configuration, new String[]{"wordcount", "/input", "/output"});
        job.submit();
        job.waitForCompletion(true);

        Assert.assertTrue(job.isSuccessful());
        File path = TestHelper.resourceAsFile(TestHelper.resourcePath("output/"));
        for (String name : Objects.requireNonNull(path.list())) {
            Assert.assertEquals(fs.open("/output/" + name),
                TestHelper.resourceAsString(TestHelper.resourcePath("output/" + name)));
        }
    }

    @Test
    public void testJobWithSkipPatterns() throws Exception {
        fs.sync(TestHelper.resourcePath("input/"), "/input");
        fs.create("/cache/patterns", "[^A-Za-z0-9 ]+");
        Configuration configuration = cluster.getConfiguration();
        configuration.setBoolean("grapes.case.sensitive", false);

        Job job = definition.defineJob(configuration, new String[]{"wordcount", "/input", "/output", "/cache/patterns"});
        job.submit();
        job.waitForCompletion(true);

        Assert.assertTrue(job.isSuccessful());
        File path = TestHelper.resourceAsFile(TestHelper.resourcePath("output/"));
        for (String name : Objects.requireNonNull(path.list())) {
            Assert.assertEquals(fs.open("/output/" + name),
                TestHelper.resourceAsString(TestHelper.resourcePath("output/" + name)));
        }
    }

}
