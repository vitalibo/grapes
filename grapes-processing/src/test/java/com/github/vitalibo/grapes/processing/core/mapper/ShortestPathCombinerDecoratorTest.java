package com.github.vitalibo.grapes.processing.core.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class ShortestPathCombinerDecoratorTest {

    @Mock
    private MultipleOutputs<IntWritable, ? extends Writable> mockMultipleOutputs;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private ShortestPathCombinerDecorator.Context mockContext;
    @Mock
    private Counter mockCounter;

    private ShortestPathCombinerDecorator mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        mapper = new ShortestPathCombinerDecorator((c) -> mockMultipleOutputs);
        Mockito.when(mockContext.getConfiguration()).thenReturn(mockConfiguration);
        Mockito.when(mockContext.getCounter(Mockito.anyString(), Mockito.anyString())).thenReturn(mockCounter);
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        IntWritable key = new IntWritable(12);
        ArrayPrimitiveWritable value = new ArrayPrimitiveWritable(new int[]{1, 2});

        mapper.setup(mockContext);
        mapper.map(key, value, mockContext);

        Mockito.verify(mockContext).write(key, value);
        Mockito.verify(mockCounter, Mockito.never()).increment(Mockito.anyLong());
        Mockito.verify(mockMultipleOutputs, Mockito.never()).write("sixdegrees", key, value);
    }

    @Test
    public void testMapSkipIncrement() throws IOException, InterruptedException {
        IntWritable key = new IntWritable(12);
        Mockito.when(mockConfiguration.get("grapes.vertex.target")).thenReturn("11");
        ArrayPrimitiveWritable value = new ArrayPrimitiveWritable(new int[]{1, 2});

        mapper.setup(mockContext);
        mapper.map(key, value, mockContext);

        Mockito.verify(mockContext).write(key, value);
        Mockito.verify(mockCounter, Mockito.never()).increment(Mockito.anyLong());
        Mockito.verify(mockMultipleOutputs, Mockito.never()).write("sixdegrees", key, value);
    }

    @Test
    public void testMapIncrement() throws IOException, InterruptedException {
        IntWritable key = new IntWritable(12);
        Mockito.when(mockConfiguration.get("grapes.vertex.target")).thenReturn("12");
        ArrayPrimitiveWritable value = new ArrayPrimitiveWritable(new int[]{1, 2});

        mapper.setup(mockContext);
        mapper.map(key, value, mockContext);

        Mockito.verify(mockContext).write(key, value);
        Mockito.verify(mockCounter).increment(Mockito.anyLong());
        Mockito.verify(mockMultipleOutputs).write("sixdegrees", key, value);
    }

    @Test
    public void testCleanup() throws IOException, InterruptedException {
        mapper.setup(mockContext);
        mapper.cleanup(mockContext);

        Mockito.verify(mockMultipleOutputs).close();
    }

}