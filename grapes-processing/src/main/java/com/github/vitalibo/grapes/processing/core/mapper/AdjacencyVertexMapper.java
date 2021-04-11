package com.github.vitalibo.grapes.processing.core.mapper;

import com.github.vitalibo.grapes.processing.core.io.model.VisitedVertexWritable;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@RequiredArgsConstructor
public class AdjacencyVertexMapper extends Mapper<IntWritable, VisitedVertexWritable, IntWritable, ArrayPrimitiveWritable> {

    private final IntWritable outKey;
    private final ArrayPrimitiveWritable outValue;

    public AdjacencyVertexMapper() {
        this(new IntWritable(), new ArrayPrimitiveWritable());
    }

    @Override
    public void map(IntWritable inKey, VisitedVertexWritable inValue, Context context) throws IOException, InterruptedException {
        final int[] path = inValue.getPath();

        outValue.set(path);
        context.write(inKey, outValue);

        int[] npath = new int[path.length + 1];
        npath[0] = inKey.get();
        System.arraycopy(path, 0, npath, 1, path.length);
        outValue.set(npath);

        for (int neighbour : inValue.getNeighbours()) {
            outKey.set(neighbour);
            context.write(outKey, outValue);
        }
    }

}
