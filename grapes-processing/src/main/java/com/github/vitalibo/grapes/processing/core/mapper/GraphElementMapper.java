package com.github.vitalibo.grapes.processing.core.mapper;

import com.github.vitalibo.grapes.processing.core.io.model.EdgeWritable;
import com.github.vitalibo.grapes.processing.core.io.model.GraphElementWritable;
import com.github.vitalibo.grapes.processing.core.io.model.VertexWritable;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@RequiredArgsConstructor
public class GraphElementMapper extends Mapper<IntWritable, ArrayPrimitiveWritable, IntWritable, GraphElementWritable> {

    private final IntWritable chunk;
    private final GraphElementWritable element;
    private final VertexWritable vertex;
    private final EdgeWritable edge;

    private Context context;
    private int chunkSize;

    public GraphElementMapper() {
        this(new IntWritable(), new GraphElementWritable(), new VertexWritable(), new EdgeWritable());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.chunkSize = configuration.getInt("grapes.mapper.chunkSize", 100_000);
        this.context = context;
    }

    @Override
    public void map(IntWritable key, ArrayPrimitiveWritable value, Context context) throws IOException, InterruptedException {
        int rootNode = key.get();
        writeVertex(rootNode);

        for (int edgeNode : (int[]) value.get()) {
            writeVertex(edgeNode);
            writeEdge(rootNode, edgeNode);
        }
    }

    private void writeVertex(int node) throws IOException, InterruptedException {
        chunk.set(node / chunkSize);
        vertex.setNode(node);
        element.set(vertex);
        context.write(chunk, element);
    }

    private void writeEdge(int node1, int node2) throws IOException, InterruptedException {
        chunk.set(Math.min(node1, node2) / chunkSize);
        edge.setSourceNode(Math.min(node1, node2));
        edge.setTargetNode(Math.max(node1, node2));
        element.set(edge);
        context.write(chunk, element);
    }

}
