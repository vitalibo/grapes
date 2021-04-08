package com.github.vitalibo.grapes.processing.core.io.model;

import com.github.vitalibo.grapes.processing.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class VertexWritableTest {

    @Test
    public void testSerDe() throws IOException {
        VertexWritable original = new VertexWritable(123);

        VertexWritable actual = TestHelper.serDe(original, new VertexWritable());

        Assert.assertNotSame(actual, original);
        Assert.assertEquals(actual.getNode(), 123);
    }

}