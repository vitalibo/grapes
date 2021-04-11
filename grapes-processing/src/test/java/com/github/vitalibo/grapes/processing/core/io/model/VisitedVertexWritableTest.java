package com.github.vitalibo.grapes.processing.core.io.model;

import com.github.vitalibo.grapes.processing.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class VisitedVertexWritableTest {

    @Test
    public void testSerDe() throws IOException {
        VisitedVertexWritable original = new VisitedVertexWritable();
        original.setNeighbours(new int[]{1, 2, 3});
        original.setPath(new int[]{9, 8});

        VisitedVertexWritable actual = TestHelper.serDe(original, new VisitedVertexWritable());

        Assert.assertNotSame(actual, original);
        Assert.assertEquals(actual.getNeighbours(), new int[]{1, 2, 3});
        Assert.assertEquals(actual.getPath(), new int[]{9, 8});
    }

    @Test
    public void testSerDeEmpty() throws IOException {
        VisitedVertexWritable original = new VisitedVertexWritable();
        original.setNeighbours(new int[0]);
        original.setPath(new int[0]);

        VisitedVertexWritable actual = TestHelper.serDe(original, new VisitedVertexWritable());

        Assert.assertNotSame(actual, original);
        Assert.assertEquals(actual.getNeighbours(), new int[0]);
        Assert.assertEquals(actual.getPath(), new int[0]);
    }

}
