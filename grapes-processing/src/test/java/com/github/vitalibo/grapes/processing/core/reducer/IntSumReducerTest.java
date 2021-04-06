package com.github.vitalibo.grapes.processing.core.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

public class IntSumReducerTest {

    @Mock
    private IntWritable mockIntWritable;
    @Mock
    private Text mockText;
    @Mock
    private IntWritable mockIntWritableOut;
    @Mock
    private IntSumReducer.Context mockContext;

    private IntSumReducer reducer;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        reducer = new IntSumReducer(mockIntWritableOut);
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Mockito.when(mockIntWritable.get()).thenReturn(2, 5, 4);

        reducer.reduce(mockText, Arrays.asList(mockIntWritable, mockIntWritable, mockIntWritable), mockContext);

        Mockito.verify(mockIntWritableOut).set(11);
        Mockito.verify(mockContext).write(mockText, mockIntWritableOut);
    }

}