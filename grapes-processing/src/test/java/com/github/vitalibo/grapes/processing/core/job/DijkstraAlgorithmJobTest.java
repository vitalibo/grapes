package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.MapReduceSuiteBase;
import com.github.vitalibo.grapes.processing.TestHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class DijkstraAlgorithmJobTest extends MapReduceSuiteBase {

    private final DijkstraAlgorithmJob definition = new DijkstraAlgorithmJob();

    @Test
    public void testDefineJobControl() throws Exception {
        Configuration configuration = cluster.getConfiguration();
        configuration.set("grapes.vertex.target", "5197");
        fs.createSequenceFile("input/part-m-00000");
        fs.createTextFile("phase2/out/cache-r-00001");
        fs.createSequenceFile("phase2/out/part-r-00000");

        JobControl jobControl = definition.defineJobControl(configuration, new String[]{"dijkstra", "3", "/input", ""});
        final Thread thread = new Thread(jobControl);
        thread.setDaemon(true);
        thread.start();
        while (!jobControl.allFinished()) {
            Thread.sleep(1000);
        }

        Assert.assertTrue(jobControl.getFailedJobList().isEmpty());
        fs.assertEqualsSequenceFile("phase3/out/part-r-00000");
        Assert.assertEquals(fs.open("/phase3/out/sixdegrees-r-00000.xml"),
            TestHelper.resourceAsString(TestHelper.resourcePath("phase3/out/sixdegrees-r-00000.xml")));
    }

    @Test
    public void testCreateInitialVertex() throws IOException {
        Configuration configuration = cluster.getConfiguration();
        configuration.set("grapes.vertex.initial", "123");

        DijkstraAlgorithmJob.defineSearchPreparationJob(configuration, 1, "in", "/phase");

        Assert.assertEquals(fs.open("/phase0/out/cache-r-00000"), "123");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateInitialVertexThrowException() throws IOException {
        Configuration configuration = cluster.getConfiguration();

        DijkstraAlgorithmJob.defineSearchPreparationJob(configuration, 1, "in", "/phase");
    }

}
