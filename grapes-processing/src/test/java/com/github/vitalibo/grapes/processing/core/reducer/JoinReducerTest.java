package com.github.vitalibo.grapes.processing.core.reducer;

import com.github.vitalibo.grapes.processing.core.io.model.MultipleInputWritable;
import com.github.vitalibo.grapes.processing.core.io.model.VisitedVertexWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class JoinReducerTest {

    @Mock
    private IntWritable mockIntWritable;
    @Mock
    private MultipleInputWritable<ArrayPrimitiveWritable> mockMultipleInputWritable;
    @Mock
    private JoinReducer.Context mockContext;

    private VisitedVertexWritable visitedVertexWritable;
    private JoinReducer reducer;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        visitedVertexWritable = new VisitedVertexWritable();
        visitedVertexWritable.setPath(new int[]{1});
        visitedVertexWritable.setNeighbours(new int[]{2});
        reducer = new JoinReducer(visitedVertexWritable);
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Mockito.when(mockMultipleInputWritable.getType()).thenReturn((byte) 1, (byte) 2);
        Mockito.when(mockMultipleInputWritable.getInstance())
            .thenReturn(new ArrayPrimitiveWritable(new int[]{11, 12}), new ArrayPrimitiveWritable(new int[]{21, 22}));

        reducer.reduce(mockIntWritable, Arrays.asList(mockMultipleInputWritable, mockMultipleInputWritable), mockContext);

        Mockito.verify(mockContext).write(mockIntWritable, visitedVertexWritable);
        Assert.assertEquals(visitedVertexWritable.getNeighbours(), new int[]{11, 12});
        Assert.assertEquals(visitedVertexWritable.getPath(), new int[]{21, 22});
    }

    @Test
    public void testReduceEmpty() throws IOException, InterruptedException {
        reducer.reduce(mockIntWritable, Collections.emptyList(), mockContext);

        Mockito.verify(mockContext).write(mockIntWritable, visitedVertexWritable);
        Assert.assertEquals(visitedVertexWritable.getNeighbours(), new int[0]);
        Assert.assertEquals(visitedVertexWritable.getPath(), new int[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testReduceUnknownType() throws IOException, InterruptedException {
        Mockito.when(mockMultipleInputWritable.getType()).thenReturn((byte) 3);
        Mockito.when(mockMultipleInputWritable.getInstance()).thenReturn(new ArrayPrimitiveWritable(new int[]{11, 12}));

        reducer.reduce(mockIntWritable, Arrays.asList(mockMultipleInputWritable, mockMultipleInputWritable), mockContext);
    }

}
