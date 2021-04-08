package com.github.vitalibo.grapes.processing.core.io.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VertexWritable implements WritableComparable<VertexWritable> {

    private int node;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(node);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        node = in.readInt();
    }

    @Override
    public int compareTo(VertexWritable that) {
        return Integer.compare(this.node, that.node);
    }

}
