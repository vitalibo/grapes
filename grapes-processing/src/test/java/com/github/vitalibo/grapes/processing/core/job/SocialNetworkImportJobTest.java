package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.MapReduceSuiteBase;
import com.github.vitalibo.grapes.processing.TestHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SocialNetworkImportJobTest extends MapReduceSuiteBase {

    private SocialNetworkImportJob definition;

    @BeforeClass
    public void setUp() {
        definition = new SocialNetworkImportJob();
    }

    @Test
    public void testJob() throws Exception {
        System.err.println(TestHelper.resourcePath("output/"));
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
        assertSequenceFileEquals(configuration,
            "/output/part-m-00000", TestHelper.resourcePath("output/part-m-00000.txt"));
        assertSequenceFileEquals(configuration,
            "/output/part-m-00001", TestHelper.resourcePath("output/part-m-00001.txt"));
    }

    private static void assertSequenceFileEquals(Configuration configuration, String remotePath, String expectedPath) throws IOException {
        Map<Integer, int[]> actual = parseSequenceFile(configuration, remotePath);
        Map<Integer, int[]> expected = parseText(expectedPath);

        Assert.assertEquals(actual.size(), expected.size());
        for (Map.Entry<Integer, int[]> entry : expected.entrySet()) {
            Assert.assertEquals(actual.get(entry.getKey()), entry.getValue());
        }
    }

    private static Map<Integer, int[]> parseSequenceFile(Configuration configuration, String path) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(new Path(path)));
        IntWritable key = new IntWritable();
        ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();
        Map<Integer, int[]> result = new HashMap<>();
        while (reader.next(key, value)) {
            result.put(key.get(), (int[]) value.get());
        }

        return result;
    }

    private static Map<Integer, int[]> parseText(String path) {
        return new BufferedReader(new InputStreamReader(TestHelper.resourceAsInputStream(path)))
            .lines()
            .map(i -> i.split(" - "))
            .collect(Collectors.toMap(
                i -> Integer.parseInt(i[0].trim()),
                i -> i.length > 1 ? Arrays.stream(i[1]
                    .split(" "))
                    .mapToInt(Integer::parseInt)
                    .toArray() : new int[0]));
    }

}
