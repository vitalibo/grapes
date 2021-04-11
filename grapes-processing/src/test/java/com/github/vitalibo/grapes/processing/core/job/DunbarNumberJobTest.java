package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.MapReduceSuiteBase;
import com.github.vitalibo.grapes.processing.TestHelper;
import org.apache.hadoop.mapreduce.Job;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class DunbarNumberJobTest extends MapReduceSuiteBase {

    private final DunbarNumberJob definition = new DunbarNumberJob();

    @Test
    public void testJob() throws Exception {
        fs.createSequenceFile("input/part-m-00000");

        Job job = definition.defineJob(cluster.getConfiguration(), new String[]{"dunbar", "/input", "/output"});
        job.submit();
        job.waitForCompletion(true);

        Assert.assertTrue(job.isSuccessful());
        assertOutputEquals("output/");
    }

    private void assertOutputEquals(String fpath) throws IOException {
        File path = TestHelper.resourceAsFile(TestHelper.resourcePath(fpath, 3));
        for (String name : Objects.requireNonNull(path.list())) {
            Assert.assertEquals(fs.open("/" + fpath + name),
                TestHelper.resourceAsString(TestHelper.resourcePath(fpath + name, 3)));
        }
    }

}
