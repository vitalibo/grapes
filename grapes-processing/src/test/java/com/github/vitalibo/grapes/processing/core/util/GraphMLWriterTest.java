package com.github.vitalibo.grapes.processing.core.util;

import com.github.vitalibo.grapes.processing.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GraphMLWriterTest {

    @Test
    public void testWrite() throws XMLStreamException, IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1);
        GraphMLWriter writer = new GraphMLWriter(bos);

        writer.createStartDocument();
        writer.createNodeKey("name", "name", "string");
        writer.createNodeKey("gender", "gender", "string");
        writer.createEdgeKey("relation", "relation", "string");
        writer.createNode(1, attributes("name", "Jeff", "gender", "M"));
        writer.createNode(2, attributes("name", "Ed", "gender", "M"));
        writer.createNode(3, attributes("name", "Christiaan", "gender", "M"));
        writer.createNode(4, attributes("name", "Emily", "gender", "F"));
        writer.createNode(5, attributes("name", "Adam", "gender", "M"));
        writer.createNode(6, attributes("name", "Cynthia", "gender", "F"));
        writer.createEdge(1, 2, attributes("relation", "brother"));
        writer.createEdge(1, 4, attributes("relation", "wife"));
        writer.createEdge(1, 3);
        writer.createEdge(1, 5, attributes("relation", "son"));
        writer.createEdge(4, 5, attributes("relation", "son"));
        writer.createEdge(1, 6, attributes("relation", "daughter"));
        writer.createEdge(4, 6, attributes("relation", "daughter"));
        writer.createEndElement();
        writer.close();

        Assert.assertEquals(bos.toString(), TestHelper.resourceAsString(TestHelper.resourcePath("socialnet.xml")));
    }

    private static Map<String, String> attributes(String... a) {
        Map<String, String> s = new HashMap<>();
        for (int i = 0; i < a.length; i += 2) {
            s.put(a[i], a[i + 1]);
        }
        return s;
    }

}
