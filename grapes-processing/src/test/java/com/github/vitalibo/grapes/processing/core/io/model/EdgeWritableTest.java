package com.github.vitalibo.grapes.processing.core.io.model;

import com.github.vitalibo.grapes.processing.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class EdgeWritableTest {

    @Test
    public void testSerDe() throws IOException {
        EdgeWritable original = new EdgeWritable(123, 321);

        EdgeWritable actual = TestHelper.serDe(original, new EdgeWritable());

        Assert.assertNotSame(actual, original);
        Assert.assertEquals(actual.getSourceNode(), 123);
        Assert.assertEquals(actual.getTargetNode(), 321);
    }

}