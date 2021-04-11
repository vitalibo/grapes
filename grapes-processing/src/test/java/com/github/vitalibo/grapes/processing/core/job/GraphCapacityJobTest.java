package com.github.vitalibo.grapes.processing.core.job;

import com.github.vitalibo.grapes.processing.MapReduceSuiteBase;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GraphCapacityJobTest extends MapReduceSuiteBase {

    private final GraphCapacityJob definition = new GraphCapacityJob();

    @Test
    public void testJob() throws Exception {
        fs.createSequenceFile("input/part-m-00000");
        fs.createSequenceFile("input/part-m-00001");

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

}
