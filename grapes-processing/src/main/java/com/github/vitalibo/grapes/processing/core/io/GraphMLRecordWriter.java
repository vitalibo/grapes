package com.github.vitalibo.grapes.processing.core.io;

import com.github.vitalibo.grapes.processing.core.util.GraphMLWriter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import javax.xml.stream.XMLStreamException;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor
public class GraphMLRecordWriter extends RecordWriter<IntWritable, ArrayPrimitiveWritable> {

    private final GraphMLWriter writer;
    private final Set<Integer> vertices;
    private final Set<Long> edges;

    private boolean requiredStartDocument = true;

    public GraphMLRecordWriter(DataOutputStream output) {
        this(new GraphMLWriter(output), new HashSet<>(), new HashSet<>());
    }

    @Override
    @SneakyThrows
    public synchronized void write(IntWritable key, ArrayPrimitiveWritable value) throws IOException, InterruptedException {
        if (requiredStartDocument) {
            writeStartDocument();
        }

        int root = key.get();
        int[] path = (int[]) value.get();

        writeVertex(root, path.length);
        writeVertex(path[0], path.length - 1);
        writeEdge(root, path[0]);

        for (int i = 1; i < path.length; i++) {
            writeVertex(path[i], path.length - i - 1);
            writeEdge(path[i - 1], path[i]);
        }
    }

    private void writeStartDocument() throws XMLStreamException {
        writer.createStartDocument();
        writer.createNodeKey("id", "id", "string");
        writer.createNodeKey("step", "step", "int");
        requiredStartDocument = false;
    }

    private void writeVertex(int id, int step) throws XMLStreamException {
        if (vertices.contains(id)) {
            return;
        }

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("id", id);
        attributes.put("step", step);
        writer.createNode(id, attributes);
        vertices.add(id);
    }

    private void writeEdge(int source, int target) throws XMLStreamException {
        long edge = (long) Math.min(source, target) << 32 | Math.max(source, target) & 0xFFFFFFFFL;
        if (edges.contains(edge)) {
            return;
        }

        writer.createEdge(source, target);
        edges.add(edge);
    }

    @Override
    @SneakyThrows
    public synchronized void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (requiredStartDocument) {
            writeStartDocument();
        }

        writer.createEndElement();
        writer.close();
    }

}
