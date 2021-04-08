package com.github.vitalibo.grapes.processing.core.reducer;

import com.github.vitalibo.grapes.processing.core.io.model.EdgeWritable;
import com.github.vitalibo.grapes.processing.core.io.model.GraphElementType;
import com.github.vitalibo.grapes.processing.core.io.model.GraphElementWritable;
import com.github.vitalibo.grapes.processing.core.io.model.VertexWritable;
import lombok.Data;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GraphCapacityReducer extends Reducer<IntWritable, GraphElementWritable, NullWritable, NullWritable> {

    private Counter counterVertex;
    private Counter counterEdge;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        counterVertex = context.getCounter("Graph Capacity", "Vertices");
        counterEdge = context.getCounter("Graph Capacity", "Edges");
    }

    @Override
    protected void reduce(IntWritable key, Iterable<GraphElementWritable> values, Context context) {
        final Set<Integer> vertices = new HashSet<>();
        final Set<Edge> edges = new HashSet<>();
        for (GraphElementWritable element : values) {
            if (element.getType() == GraphElementType.Edge) {
                EdgeWritable edge = (EdgeWritable) element.get();
                edges.add(new Edge(edge.getSourceNode(), edge.getTargetNode()));
            } else {
                VertexWritable vertex = (VertexWritable) element.get();
                vertices.add(vertex.getNode());
            }
        }

        counterVertex.increment(vertices.size());
        counterEdge.increment(edges.size());
    }

    @Data
    public static class Edge {

        private final int sourceNode;
        private final int targetNode;

    }

}
