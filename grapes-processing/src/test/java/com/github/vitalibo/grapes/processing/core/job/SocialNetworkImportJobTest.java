package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.MapReduceSuiteBase;
import com.github.vitalibo.grapes.processing.TestHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        assertEqualsSequenceFile("output/part-m-00000");
        assertEqualsSequenceFile("output/part-m-00001");
    }

    private void assertEqualsSequenceFile(String path) throws IOException {
        final IntWritable key = new IntWritable();
        final ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();
        SequenceFile.Reader reader = fs.openSequenceFile("/" + path);
        List<Map.Entry<Integer, int[]>> expected = TestHelper.resourceAsListPair(
            TestHelper.resourcePath(path + ".txt", 3));
        Map<Integer, int[]> actual = new HashMap<>();
        while (reader.next(key, value)) {
            actual.put(key.get(), (int[]) value.get());
        }

        Assert.assertEquals(actual.size(), expected.size());
        for (Map.Entry<Integer, int[]> entry : expected) {
            Assert.assertEquals(actual.get(entry.getKey()), entry.getValue());
        }
    }

}
