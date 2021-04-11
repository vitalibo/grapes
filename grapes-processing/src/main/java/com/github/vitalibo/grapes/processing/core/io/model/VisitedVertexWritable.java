package com.github.vitalibo.grapes.processing.core.io.model;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class VisitedVertexWritable implements Writable {

    private int[] neighbours;
    private int[] path;

    @Override
    public void write(DataOutput out) throws IOException {
        writeIntArray(out, neighbours);
        writeIntArray(out, path);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        neighbours = readIntArray(in);
        path = readIntArray(in);
    }

    private static void writeIntArray(DataOutput out, int[] items) throws IOException {
        out.writeInt(items.length);
        for (int item : items) {
            out.writeInt(item);
        }
    }

    private static int[] readIntArray(DataInput in) throws IOException {
        int[] items = new int[in.readInt()];
        for (int i = 0; i < items.length; i++) {
            items[i] = in.readInt();
        }

        return items;
    }

}
