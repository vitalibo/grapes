package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.MapReduceSuiteBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SocialNetworkImportJobTest extends MapReduceSuiteBase {

    private final SocialNetworkImportJob definition = new SocialNetworkImportJob();

    @Test
    public void testJob() throws Exception {
        Configuration configuration = cluster.getConfiguration();
        configuration.set("grapes.recordReader.class", "com.github.vitalibo.grapes.processing.infrastructure.vk.VkUserRecordReader");
        configuration.set("grapes.vk.clientClass", "com.github.vitalibo.grapes.processing.infrastructure.vk.impl.VkClientMock");
        configuration.set("grapes.vk.mock.pseudorandomSeed.0", "123");
        configuration.set("grapes.vk.mock.pseudorandomSeed.1", "234");
        configuration.set("grapes.split.numbs", "2");

        Job job = definition.defineJob(configuration, new String[]{"socnetimp", "/output"});
        job.submit();
        job.waitForCompletion(true);

        Assert.assertTrue(job.isSuccessful());
        fs.assertEqualsSequenceFile("output/part-m-00000");
        fs.assertEqualsSequenceFile("output/part-m-00001");
    }

}
