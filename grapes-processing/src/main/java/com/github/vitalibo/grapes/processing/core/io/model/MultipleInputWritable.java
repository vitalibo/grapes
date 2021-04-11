package com.github.vitalibo.grapes.processing.core.io.model;

import lombok.Data;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class MultipleInputWritable<T extends Writable> implements Writable {

    private final ByteWritable type;
    private T instance;

    public MultipleInputWritable(T instance) {
        this.type = new ByteWritable();
        this.instance = instance;
    }

    public MultipleInputWritable(byte type, T instance) {
        this.type = new ByteWritable(type);
        this.instance = instance;
    }

    public byte getType() {
        return type.get();
    }

    public MultipleInputWritable<T> wrap(T instance) {
        this.instance = instance;
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        type.write(out);
        instance.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type.readFields(in);
        instance.readFields(in);
    }

    public static class ArrayPrimitiveWritable extends MultipleInputWritable<org.apache.hadoop.io.ArrayPrimitiveWritable> {

        public ArrayPrimitiveWritable() {
            super(new org.apache.hadoop.io.ArrayPrimitiveWritable());
        }

        public ArrayPrimitiveWritable(byte type) {
            super(type, new org.apache.hadoop.io.ArrayPrimitiveWritable());
        }

    }

}
