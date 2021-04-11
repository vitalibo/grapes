package com.github.vitalibo.grapes.processing.core.mapper;

import com.github.vitalibo.grapes.processing.core.io.model.VisitedVertexWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

public class AdjacencyVertexMapperTest {

    @Mock
    private IntWritable mockIntWritable;
    @Mock
    private ArrayPrimitiveWritable mockArrayPrimitiveWritable;
    @Mock
    private AdjacencyVertexMapper.Context mockContext;

    private AdjacencyVertexMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        mapper = new AdjacencyVertexMapper(mockIntWritable, mockArrayPrimitiveWritable);
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        VisitedVertexWritable value = new VisitedVertexWritable();
        IntWritable key = new IntWritable(10);
        value.setNeighbours(new int[]{11, 12, 13});
        value.setPath(new int[]{1, 2});

        mapper.map(key, value, mockContext);

        Mockito.verify(mockArrayPrimitiveWritable).set(new int[]{1, 2});
        Mockito.verify(mockContext).write(key, mockArrayPrimitiveWritable);
        Arrays.asList(11, 12, 13).forEach(i -> Mockito.verify(mockIntWritable).set(i));
        Mockito.verify(mockArrayPrimitiveWritable).set(new int[]{10, 1, 2});
        Mockito.verify(mockContext, Mockito.times(3)).write(mockIntWritable, mockArrayPrimitiveWritable);
    }

}
