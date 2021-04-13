package com.github.vitalibo.grapes.processing.core.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ShortestPathCombinerTest {

    @Mock
    private ShortestPathCombiner.Context mockContext;
    @Mock
    private Configuration mockConfiguration;
    @Captor
    private ArgumentCaptor<ArrayPrimitiveWritable> captorArrayPrimitiveWritable;

    private ShortestPathCombiner combiner;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        combiner = new ShortestPathCombiner();
        Mockito.when(mockContext.getConfiguration()).thenReturn(mockConfiguration);
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        IntWritable key = new IntWritable(12);
        List<ArrayPrimitiveWritable> values = Arrays.asList(
            new ArrayPrimitiveWritable(new int[]{4, 3, 2, 1}),
            new ArrayPrimitiveWritable(new int[]{4, 1}),
            new ArrayPrimitiveWritable(new int[]{4, 3, 1}));

        combiner.setup(mockContext);
        combiner.reduce(key, values, mockContext);

        Mockito.verify(mockContext).write(Mockito.eq(key), captorArrayPrimitiveWritable.capture());
        ArrayPrimitiveWritable value = captorArrayPrimitiveWritable.getValue();
        Assert.assertEquals(value.get(), new int[]{4, 1});
        Mockito.verify(mockConfiguration).get("grapes.vertex.target");
    }

    @Test
    public void testReduceStoreAllPath() throws IOException, InterruptedException {
        Mockito.when(mockConfiguration.get("grapes.vertex.target")).thenReturn("12");
        IntWritable key = new IntWritable(12);
        List<ArrayPrimitiveWritable> values = Arrays.asList(
            new ArrayPrimitiveWritable(new int[]{4, 3, 2, 1}),
            new ArrayPrimitiveWritable(new int[]{4, 1}),
            new ArrayPrimitiveWritable(new int[]{4, 3, 1}));

        combiner.setup(mockContext);
        combiner.reduce(key, values, mockContext);

        Mockito.verify(mockContext, Mockito.times(3)).write(Mockito.eq(key), captorArrayPrimitiveWritable.capture());
        List<ArrayPrimitiveWritable> actual = captorArrayPrimitiveWritable.getAllValues();
        Assert.assertEquals(actual.get(0).get(), new int[]{4, 3, 2, 1});
        Assert.assertEquals(actual.get(1).get(), new int[]{4, 1});
        Assert.assertEquals(actual.get(2).get(), new int[]{4, 3, 1});
        Mockito.verify(mockConfiguration).get("grapes.vertex.target");
    }

}
