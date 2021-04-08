package com.github.vitalibo.grapes.processing.core.io.model;

import com.github.vitalibo.grapes.processing.TestHelper;
import org.apache.hadoop.io.Writable;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

public class GraphElementWritableTest {

    @DataProvider
    public Object[][] types() {
        return new Object[][]{
            {new VertexWritable(), GraphElementType.Vertex},
            {new EdgeWritable(), GraphElementType.Edge}
        };
    }

    @Test(dataProvider = "types")
    public void testGetTypeVertex(Writable writable, GraphElementType type) {
        GraphElementWritable element = new GraphElementWritable();
        element.set(writable);

        GraphElementType actual = element.getType();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, type);
    }

    @Test
    public void testSerDe() throws IOException {
        GraphElementWritable original = new GraphElementWritable(new EdgeWritable(1, 2));

        GraphElementWritable actual = TestHelper.serDe(original, new GraphElementWritable());

        Assert.assertNotSame(original, actual);
        Assert.assertEquals(actual.getType(), GraphElementType.Edge);
        EdgeWritable edge = (EdgeWritable) actual.get();
        Assert.assertNotNull(edge);
        Assert.assertEquals(edge.getSourceNode(), 1);
        Assert.assertEquals(edge.getTargetNode(), 2);
    }

}
