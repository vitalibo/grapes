package com.github.vitalibo.grapes.processing.core.reducer;

import com.github.vitalibo.grapes.processing.core.io.model.MultipleInputWritable;
import com.github.vitalibo.grapes.processing.core.io.model.VisitedVertexWritable;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

@RequiredArgsConstructor
public class JoinReducer extends Reducer<IntWritable, MultipleInputWritable<ArrayPrimitiveWritable>, IntWritable, VisitedVertexWritable> {

    private static final int[] EMPTY_ARRAY = new int[0];

    private final VisitedVertexWritable output;

    public JoinReducer() {
        this(new VisitedVertexWritable());
    }

    @Override
    protected void reduce(IntWritable key, Iterable<MultipleInputWritable<ArrayPrimitiveWritable>> values, Context context) throws IOException, InterruptedException {
        output.setNeighbours(EMPTY_ARRAY);
        output.setPath(EMPTY_ARRAY);

        for (MultipleInputWritable<ArrayPrimitiveWritable> v : values) {
            int[] array = (int[]) v.getInstance().get();
            switch (v.getType()) {
                case 1:
                    output.setNeighbours(array);
                    break;
                case 2:
                    output.setPath(array);
                    break;
                default:
                    throw new IllegalArgumentException("unknown input type");
            }
        }

        context.write(key, output);
    }

}
