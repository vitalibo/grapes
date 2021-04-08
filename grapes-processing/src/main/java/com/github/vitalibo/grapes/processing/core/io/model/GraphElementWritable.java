package com.github.vitalibo.grapes.processing.core.io.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

import java.lang.reflect.Field;

@NoArgsConstructor
public class GraphElementWritable extends GenericWritable {

    @Getter
    private final Class[] types = {VertexWritable.class, EdgeWritable.class};
    private final Field type = privateFieldType();

    public GraphElementWritable(Writable obj) {
        set(obj);
    }

    @SneakyThrows(IllegalAccessException.class)
    public GraphElementType getType() {
        return GraphElementType.values()[(byte) type.get(this)];
    }

    @SneakyThrows
    private static Field privateFieldType() {
        Field field = GenericWritable.class.getDeclaredField("type");
        field.setAccessible(true);
        return field;
    }

}
