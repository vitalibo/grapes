package com.github.vitalibo.grapes.processing.core.io;

import com.github.vitalibo.grapes.processing.core.util.GraphMLWriter;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.*;

public class GraphMLRecordWriterTest {

    @Mock
    private GraphMLWriter mockGraphMLWriter;
    @Mock
    private TaskAttemptContext mockTaskAttemptContext;

    private GraphMLRecordWriter writer;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        writer = new GraphMLRecordWriter(mockGraphMLWriter, new HashSet<>(), new HashSet<>());
    }

    @Test
    public void testWrite() throws IOException, InterruptedException, XMLStreamException {
        writer.write(new IntWritable(1), new ArrayPrimitiveWritable(new int[]{2, 3, 4, 5}));

        Mockito.verify(mockGraphMLWriter).createStartDocument();
        Mockito.verify(mockGraphMLWriter).createNodeKey("id", "id", "string");
        Mockito.verify(mockGraphMLWriter).createNodeKey("step", "step", "int");
        for (Integer node : Arrays.asList(1, 2, 3, 4, 5)) {
            Mockito.verify(mockGraphMLWriter).createNode(node, attributes(node, 5 - node));
        }
        Mockito.verify(mockGraphMLWriter).createEdge(1, 2);
        Mockito.verify(mockGraphMLWriter).createEdge(2, 3);
        Mockito.verify(mockGraphMLWriter).createEdge(3, 4);
        Mockito.verify(mockGraphMLWriter).createEdge(4, 5);
    }

    @Test
    public void testWriteIgnoreDuplicates() throws IOException, InterruptedException, XMLStreamException {
        writer.write(new IntWritable(1), new ArrayPrimitiveWritable(new int[]{2, 1}));

        Mockito.verify(mockGraphMLWriter).createStartDocument();
        Mockito.verify(mockGraphMLWriter).createNodeKey("id", "id", "string");
        Mockito.verify(mockGraphMLWriter).createNodeKey("step", "step", "int");
        Mockito.verify(mockGraphMLWriter).createNode(1, attributes(1, 2));
        Mockito.verify(mockGraphMLWriter).createNode(2, attributes(2, 1));
        Mockito.verify(mockGraphMLWriter).createEdge(1, 2);
    }

    @Test
    public void testClose() throws IOException, InterruptedException, XMLStreamException {
        writer.close(mockTaskAttemptContext);

        Mockito.verify(mockGraphMLWriter).createEndElement();
        Mockito.verify(mockGraphMLWriter).close();
    }

    private static Map<String, ?> attributes(int id, int step) {
        return new HashMap<String, Object>() {{
            put("id", id);
            put("step", step);
        }};
    }

}
