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
import java.net.URI;
import java.util.function.Function;
import java.util.stream.Stream;

public class MultipleInputFilterMapperTest {

    @Mock
    private MultipleInputWritable<ArrayPrimitiveWritable> mockMultipleInputWritable;
    @Mock
    private IntWritable mockIntWritable;
    @Mock
    private ArrayPrimitiveWritable mockArrayPrimitiveWritable;
    @Mock
    private MultipleInputFilterMapper.Context mockContext;
    @Mock
    private Function<URI[], Stream<String>> mockCacheFilesReader;

    private MultipleInputFilterMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        mapper = new MultipleInputFilterMapper(mockMultipleInputWritable, mockCacheFilesReader);
    }

    @Test
    public void testFilter() throws IOException, InterruptedException {
        Mockito.when(mockIntWritable.get()).thenReturn(11);
        Mockito.when(mockCacheFilesReader.apply(Mockito.any())).thenReturn(Stream.of("12", "23", "34"));
        Mockito.when(mockMultipleInputWritable.wrap(Mockito.any())).thenReturn(mockMultipleInputWritable);

        mapper.setup(mockContext);
        mapper.map(mockIntWritable, mockArrayPrimitiveWritable, mockContext);

        Mockito.verify(mockContext, Mockito.never()).write(mockIntWritable, mockMultipleInputWritable);
        Mockito.verify(mockMultipleInputWritable, Mockito.never()).wrap(Mockito.any());
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Mockito.when(mockIntWritable.get()).thenReturn(23);
        Mockito.when(mockCacheFilesReader.apply(Mockito.any())).thenReturn(Stream.of("12", "23", "34"));
        Mockito.when(mockMultipleInputWritable.wrap(Mockito.any())).thenReturn(mockMultipleInputWritable);

        mapper.setup(mockContext);
        mapper.map(mockIntWritable, mockArrayPrimitiveWritable, mockContext);

        Mockito.verify(mockContext).write(mockIntWritable, mockMultipleInputWritable);
        Mockito.verify(mockMultipleInputWritable).wrap(Mockito.any());
    }

}
