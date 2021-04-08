package com.github.vitalibo.grapes.processing.core.mapper;

import com.github.vitalibo.grapes.processing.core.io.model.EdgeWritable;
import com.github.vitalibo.grapes.processing.core.io.model.GraphElementWritable;
import com.github.vitalibo.grapes.processing.core.io.model.VertexWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

public class GraphElementMapperTest {

    @Mock
    private GraphElementWritable mockGraphElementWritable;
    @Mock
    private VertexWritable mockVertexWritable;
    @Mock
    private EdgeWritable mockEdgeWritable;
    @Mock
    private IntWritable mockIntWritable;
    @Mock
    private ArrayPrimitiveWritable mockArrayPrimitiveWritable;
    @Mock
    private GraphElementMapper.Context mockContext;
    @Mock
    private Configuration mockConfiguration;

    private GraphElementMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        mapper = new GraphElementMapper(mockIntWritable, mockGraphElementWritable, mockVertexWritable, mockEdgeWritable);
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Mockito.when(mockContext.getConfiguration()).thenReturn(mockConfiguration);
        Mockito.when(mockConfiguration.getInt(Mockito.anyString(), Mockito.anyInt())).thenReturn(100);
        Mockito.when(mockIntWritable.get()).thenReturn(333);
        Mockito.when(mockArrayPrimitiveWritable.get()).thenReturn(new int[]{111, 222, 444});

        mapper.setup(mockContext);
        mapper.map(mockIntWritable, mockArrayPrimitiveWritable, mockContext);

        Mockito.verify(mockContext, Mockito.times(7)).write(mockIntWritable, mockGraphElementWritable);
        Arrays.asList(111, 222, 333, 444).forEach(i -> Mockito.verify(mockVertexWritable).setNode(i));
        Mockito.verify(mockGraphElementWritable, Mockito.times(4)).set(mockVertexWritable);
        Mockito.verify(mockGraphElementWritable, Mockito.times(3)).set(mockEdgeWritable);
        Arrays.asList(1, 2, 3).forEach(i -> Mockito.verify(mockIntWritable, Mockito.times(2)).set(i));
        Mockito.verify(mockIntWritable).set(4);
    }

}
