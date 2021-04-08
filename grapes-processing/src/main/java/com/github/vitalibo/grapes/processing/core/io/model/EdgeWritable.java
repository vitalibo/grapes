package com.github.vitalibo.grapes.processing.core.io.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EdgeWritable implements WritableComparable<EdgeWritable> {

    private int sourceNode;
    private int targetNode;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sourceNode);
        out.writeInt(targetNode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sourceNode = in.readInt();
        targetNode = in.readInt();
    }

    @Override
    public int compareTo(EdgeWritable that) {
        return Comparator
            .comparing(EdgeWritable::getSourceNode)
            .thenComparing(EdgeWritable::getTargetNode)
            .compare(this, that);
    }

}
