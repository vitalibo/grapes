package com.github.vitalibo.grapes.processing.core.mapper;

import com.github.vitalibo.grapes.processing.core.io.model.MultipleInputWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class MultipleInputMapperTest {

    @Mock
    private MultipleInputWritable<ArrayPrimitiveWritable> mockMultipleInputWritable;
    @Mock
    private IntWritable mockIntWritable;
    @Mock
    private ArrayPrimitiveWritable mockArrayPrimitiveWritable;
    @Mock
    private MultipleInputMapper.Context mockContext;

    private MultipleInputMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        mapper = new MultipleInputMapper(mockMultipleInputWritable);
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Mockito.when(mockMultipleInputWritable.wrap(Mockito.any()))
            .thenReturn(mockMultipleInputWritable);

        mapper.map(mockIntWritable, mockArrayPrimitiveWritable, mockContext);

        Mockito.verify(mockContext).write(mockIntWritable, mockMultipleInputWritable);
        Mockito.verify(mockMultipleInputWritable).wrap(mockArrayPrimitiveWritable);
    }

}
