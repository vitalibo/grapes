package com.github.vitalibo.grapes.processing.core.reducer;

import com.github.vitalibo.grapes.processing.core.io.model.EdgeWritable;
import com.github.vitalibo.grapes.processing.core.io.model.GraphElementWritable;
import com.github.vitalibo.grapes.processing.core.io.model.VertexWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.stream.Stream;

import static com.github.vitalibo.grapes.processing.core.io.model.GraphElementType.Edge;
import static com.github.vitalibo.grapes.processing.core.io.model.GraphElementType.Vertex;

public class GraphCapacityReducerTest {

    @Mock
    private Counter mockCounterVertex;
    @Mock
    private Counter mockCounterEdge;
    @Mock
    private GraphCapacityReducer.Context mockContext;
    @Mock
    private IntWritable mockIntWritable;
    @Mock
    private GraphElementWritable mockGraphElementWritable;
    @Mock
    private Iterable<GraphElementWritable> mockIterableGraphElementWritable;
    @Mock
    private EdgeWritable mockEdgeWritable;
    @Mock
    private VertexWritable mockVertexWritable;

    private GraphCapacityReducer reducer;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        reducer = new GraphCapacityReducer();
        Mockito.when(mockContext.getCounter(Mockito.anyString(), Mockito.eq("Vertices")))
            .thenReturn(mockCounterVertex);
        Mockito.when(mockContext.getCounter(Mockito.anyString(), Mockito.eq("Edges")))
            .thenReturn(mockCounterEdge);
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Mockito.when(mockIterableGraphElementWritable.iterator())
            .thenReturn(Stream.generate(() -> mockGraphElementWritable).limit(7).iterator());
        Mockito.when(mockGraphElementWritable.getType()).thenReturn(Edge, Vertex, Edge, Edge, Vertex, Vertex, Vertex);
        Mockito.when(mockGraphElementWritable.get()).thenReturn(mockEdgeWritable, mockVertexWritable, mockEdgeWritable,
            mockEdgeWritable, mockVertexWritable, mockVertexWritable, mockVertexWritable);
        Mockito.when(mockEdgeWritable.getSourceNode()).thenReturn(1, 2, 1);
        Mockito.when(mockEdgeWritable.getTargetNode()).thenReturn(2, 1, 2);
        Mockito.when(mockVertexWritable.getNode()).thenReturn(4, 1, 2, 1);

        reducer.setup(mockContext);
        reducer.reduce(mockIntWritable, mockIterableGraphElementWritable, mockContext);

        Mockito.verify(mockCounterEdge).increment(2);
        Mockito.verify(mockCounterVertex).increment(3);
    }

}
