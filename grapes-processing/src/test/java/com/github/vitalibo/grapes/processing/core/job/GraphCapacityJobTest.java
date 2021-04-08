package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.MapReduceSuiteBase;
import com.github.vitalibo.grapes.processing.TestHelper;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GraphCapacityJobTest extends MapReduceSuiteBase {

    private final GraphCapacityJob definition = new GraphCapacityJob();

    @Test
    public void testJob() throws Exception {
        createSequenceFile("part-m-00000");
        createSequenceFile("part-m-00001");

        Job job = definition.defineJob(cluster.getConfiguration(), new String[]{"capacity", "/input"});
        job.submit();
        job.waitForCompletion(true);

        Assert.assertTrue(job.isSuccessful());
        Counters counters = job.getCounters();
        CounterGroup group = counters.getGroup("Graph Capacity");
        Counter vertices = group.findCounter("Vertices");
        Assert.assertEquals(vertices.getValue(), 9995);
        Counter edges = group.findCounter("Edges");
        Assert.assertEquals(edges.getValue(), 72833);
    }

    public void createSequenceFile(String target) throws IOException {
        final IntWritable key = new IntWritable();
        final ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();
        SequenceFile.Writer writer = fs.createSequenceFile(
            String.format("/input/%s", target), IntWritable.class, ArrayPrimitiveWritable.class);
        List<Map.Entry<Integer, int[]>> entries = TestHelper.resourceAsListPair(
            TestHelper.resourcePath(String.format("input/%s.txt", target), 3));
        for (Map.Entry<Integer, int[]> entry : entries) {
            key.set(entry.getKey());
            value.set(entry.getValue());
            writer.append(key, value);
        }

        writer.hflush();
        writer.close();
    }

}
